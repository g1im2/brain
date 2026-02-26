"""
Integration Service 定时任务调度器

基于asyncron实现的定时任务管理系统。
"""

import asyncio
import logging
import time
from datetime import datetime, date, timedelta, time as dt_time
from typing import Dict, List, Any, Optional, Callable
import uuid


from asyncron import (
    Scheduler,
    BluePrint,
    PlansAt,
    PlansEvery,
    Timer,
    TimeUnit,
    TimeUnits,
    start_scheduler
)

from config import IntegrationConfig
from exceptions import AdapterException

logger = logging.getLogger(__name__)

try:
    from adapters.flowhub_adapter import FlowhubAdapter
except Exception:
    FlowhubAdapter = None


class IntegrationScheduler:
    """Integration Service 定时任务调度器"""

    STRUCTURE_FLOWHUB_CRON_TEMPLATES: Dict[str, str] = {
        "brain_industry_board_fetch": "10 16 * * 1-5",
        "brain_concept_board_fetch": "20 16 * * 1-5",
        "brain_industry_board_stocks_fetch": "40 16 * * 1-5",
        "brain_concept_board_stocks_fetch": "55 16 * * 1-5",
        "brain_industry_moneyflow_fetch": "10 17 * * 1-5",
        "brain_concept_moneyflow_fetch": "15 17 * * 1-5",
        "brain_batch_daily_basic_fetch": "30 17 * * 1-5",
    }

    def __init__(self, config: IntegrationConfig, coordinator=None, app=None):
        """初始化调度器

        Args:
            config: 集成配置对象
            coordinator: 系统协调器实例
            app: aiohttp应用实例
        """
        self.config = config
        self.coordinator = coordinator
        self.app = app
        self._is_running = False
        self._task_history: List[Dict[str, Any]] = []
        self._blueprint = None
        # 自定义任务注册表: task_id -> {name, cron, function, enabled, payload}
        self._managed_tasks: Dict[str, Dict[str, Any]] = {}
        self._daily_fetch_retry_task: Optional[asyncio.Task] = None
        self._daily_fetch_retry_count = 0
        self._task_states: Dict[str, Dict[str, Any]] = {}
        self._poll_lock = asyncio.Lock()
        self._state_loaded_keys: set[str] = set()
        self._state_ttl_seconds = 30 * 24 * 3600

        logger.info("IntegrationScheduler initialized")

    async def start(self):
        """启动调度器"""
        if self._is_running:
            logger.info("Scheduler is already running")
            return

        try:

            # 创建任务蓝图
            self._blueprint = self._create_task_blueprint()

            # 启动asyncron调度器
            start_scheduler([self._blueprint])

            self._is_running = True
            logger.info("IntegrationScheduler started successfully")

        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            raise

    def get_planer(self) -> BluePrint:
        """获取asyncron planer（蓝图）"""
        if not hasattr(self, '_blueprint') or self._blueprint is None:
            self._blueprint = self._create_task_blueprint()
        return self._blueprint

    def _create_task_blueprint(self) -> BluePrint:
        """创建任务蓝图: 包含默认任务 + 启用的自定义任务"""
        blueprint = BluePrint()

        # 默认任务：每日数据抓取（可配置cron，缺省每天一次）
        daily_cron = getattr(self.config.service, 'daily_data_fetch_cron', None)
        daily_plans = self._parse_cron_to_plans(daily_cron) if daily_cron else PlansEvery([TimeUnits.DAYS], [1])
        blueprint.task('/daily_data_fetch', plans=daily_plans)(
            self._trigger_daily_data_fetch
        )

        # 新增任务：每日指数数据抓取
        blueprint.task('/daily_index_fetch', plans=daily_plans)(
            self._trigger_daily_index_fetch
        )

        # 新增任务：市场快照支撑数据抓取（交易日历/停复牌/申万行业映射）
        blueprint.task('/daily_market_snapshot_support_fetch', plans=daily_plans)(
            self._trigger_daily_market_snapshot_support_fetch
        )

        # 新增任务：申万行业映射月度全量校验（仅每月1日执行 full_update）
        monthly_sw_cron = getattr(self.config.service, 'monthly_sw_industry_full_fetch_cron', None) or "at:03:20"
        monthly_sw_plans = self._parse_cron_to_plans(monthly_sw_cron)
        blueprint.task('/monthly_sw_industry_full_fetch', plans=monthly_sw_plans)(
            self._trigger_monthly_sw_industry_full_fetch
        )

        # 新增任务：每日板块数据抓取
        blueprint.task('/daily_board_fetch', plans=daily_plans)(
            self._trigger_daily_board_fetch
        )

        # 新增任务：每日收盘后生成 T+1 策略建议单
        strategy_plan_cron = getattr(self.config.service, 'strategy_plan_generation_cron', None) or "at:18:50"
        strategy_plan_plans = self._parse_cron_to_plans(strategy_plan_cron)
        blueprint.task('/daily_strategy_plan_generation', plans=strategy_plan_plans)(
            self._trigger_daily_strategy_plan_generation
        )

        # 默认任务：系统健康检查（每30分钟执行一次）
        blueprint.task('/system_health_check', plans=PlansEvery([TimeUnits.MINUTES], [30]))(
            self._system_health_check
        )

        # 轮询任务：每分钟检查是否有错过的任务需要补触发
        blueprint.task('/scheduler_tick', plans=PlansEvery([TimeUnits.MINUTES], [1]))(
            self._scheduler_tick
        )

        # 合并自定义任务（启用状态）
        for task_id, t in self._managed_tasks.items():
            if not t.get('enabled', True):
                continue
            plans = self._parse_cron_to_plans(t.get('cron'))
            url = f"/custom/{task_id}"

            async def _runner(context, _tid=task_id):
                await self._execute_custom_task(self._managed_tasks.get(_tid, {}))

            blueprint.task(url, plans=plans)(_runner)

        logger.info("Task blueprint created with default and custom tasks")
        return blueprint

    def _custom_task_id(self, task_id: str) -> str:
        """将自定义任务ID转换为调度器内部ID"""
        return f"/custom/{task_id}"

    def _strip_custom_prefix(self, scheduler_task_id: str) -> str:
        """从调度器ID还原自定义任务ID"""
        if scheduler_task_id.startswith("/custom/"):
            return scheduler_task_id[len("/custom/"):]
        return scheduler_task_id

    def _calculate_time(self, time_str: str) -> datetime.time:
        """解析 HH:MM[:SS] 到 time 对象"""
        time_values = time_str.split(':')
        if len(time_values) == 3:
            hour, minute, second = time_values
        else:
            hour, minute = time_values
            second = 0
        return datetime.time(hour=int(hour), minute=int(minute), second=int(second))

    def _sync_tasks_from_plans(self) -> None:
        """将新增的plan同步为实际任务（避免重复创建）"""
        scheduler = Scheduler.get_instance()
        task_man = getattr(scheduler, "_Scheduler__task_man", None)
        if task_man is None:
            return

        existing_names = {task.get_task_name() for task in scheduler.list_tasks()}
        for url, task_opts in scheduler.tasks_opts_repo.items():
            if url in existing_names:
                continue
            plans = task_opts.get('plans')
            if isinstance(plans, PlansAt):
                for time_unit, moment in plans.list_plans():
                    at_moment = self._calculate_time(moment).isoformat()
                    plan_timer = Timer(**task_opts).set_unit(time_unit).at(at_moment)
                    task_man.add_task(task_opts['method'], url, {}, plan_timer)
            elif isinstance(plans, PlansEvery):
                for interval, units in plans.list_plans():
                    plan_timer = Timer(**task_opts).every(interval).set_units(units)
                    task_man.add_task(task_opts['method'], url, {}, plan_timer)
            else:
                task_man.add_task(task_opts['method'], url, {})

    def _refresh_task_id_mapping(self) -> None:
        """刷新自定义任务与调度器任务ID的映射"""
        scheduler = Scheduler.get_instance()
        for task in scheduler.list_tasks():
            task_name = task.get_task_name()
            if not task_name.startswith("/custom/"):
                continue
            task_id = task_name[len("/custom/"):]
            if task_id in self._managed_tasks:
                self._managed_tasks[task_id]['scheduler_task_id'] = task.get_task_id()

    async def stop(self):
        """停止调度器"""
        if not self._is_running:
            logger.info("Scheduler is not running")
            return

        try:
            # 停止asyncron调度器（兼容本地实现）
            success = False
            try:
                sched = Scheduler.get_instance()
                loop_thread = getattr(sched, "_Scheduler__loop_thread", None)
                if loop_thread and hasattr(loop_thread, "cancel"):
                    success = loop_thread.cancel()
            except Exception:
                success = False
            if success:
                self._is_running = False
                logger.info("IntegrationScheduler stopped successfully")
            else:
                logger.warning("Failed to stop scheduler gracefully")

        except Exception as e:
            logger.error(f"Failed to stop scheduler: {e}")
            raise

    def _parse_cron_to_plans(self, cron: Optional[str]) -> Any:
        """将简化的cron表达式转换为asyncron的Plans对象
        支持格式：
        - every:<n>s|m|h|d  例如 every:30m, every:1h
        - at:HH:MM[:SS]    例如 at:02:00 或 at:02:00:00 （按天）
        """
        if not cron or not isinstance(cron, str):
            # 默认每天一次
            return PlansEvery([TimeUnits.DAYS], [1])
        try:
            cron = cron.strip().lower()
            if cron.startswith('every:'):
                val = cron.split(':', 1)[1]
                num_str = ''.join(ch for ch in val if ch.isdigit()) or '1'
                unit = ''.join(ch for ch in val if ch.isalpha()) or 'm'
                n = int(num_str)
                if unit in ('s', 'sec', 'secs', 'second', 'seconds'):
                    return PlansEvery([TimeUnits.SECONDS], [n])
                if unit in ('m', 'min', 'mins', 'minute', 'minutes'):
                    return PlansEvery([TimeUnits.MINUTES], [n])
                if unit in ('h', 'hour', 'hours'):
                    return PlansEvery([TimeUnits.HOURS], [n])
                if unit in ('d', 'day', 'days'):
                    return PlansEvery([TimeUnits.DAYS], [n])
                # 默认按分钟
                return PlansEvery([TimeUnits.MINUTES], [n])
            if cron.startswith('at:'):
                # asyncron的"at"按天触发，使用PlansEvery + Timer.at 需要通过PlansAt来表达具体时间
                time_str = cron.split(':', 1)[1]
                time_str = time_str if time_str.count(':') == 2 else (time_str + ":00")
                return PlansAt([TimeUnit.DAY], [time_str])
        except Exception:
            pass
        # 兜底：每小时一次
        return PlansEvery([TimeUnits.HOURS], [1])

    def _state_key(self, task_name: str, task_id: Optional[str] = None) -> str:
        return task_id or task_name

    def _get_task_state(self, key: str) -> Dict[str, Any]:
        state = self._task_states.get(key)
        if state is None:
            state = {
                "running": False,
                "last_started_at": None,
                "last_completed_at": None,
                "last_status": None,
                "last_success_date": None,
                "last_attempt_date": None,
                "failure_count": 0,
                "last_updated_at": None
            }
            self._task_states[key] = state
        return state

    def _schedule_state_persist(self, key: str) -> None:
        if not self.app:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self._persist_task_state(key))

    async def _persist_task_state(self, key: str) -> None:
        if not self.app:
            return
        redis = self.app.get('redis')
        if not redis:
            return
        state = self._task_states.get(key)
        if not state:
            return
        payload = {
            "running": "1" if state.get("running") else "0",
            "last_started_at": state.get("last_started_at") or "",
            "last_completed_at": state.get("last_completed_at") or "",
            "last_status": state.get("last_status") or "",
            "last_success_date": state.get("last_success_date") or "",
            "last_attempt_date": state.get("last_attempt_date") or "",
            "failure_count": str(state.get("failure_count", 0)),
            "last_updated_at": datetime.utcnow().isoformat()
        }
        try:
            await redis.hset(f"scheduler:task_state:{key}", mapping=payload)
            await redis.expire(f"scheduler:task_state:{key}", self._state_ttl_seconds)
            await redis.sadd("scheduler:task_state_keys", key)
        except Exception as e:
            logger.warning(f"Failed to persist task state for {key}: {e}")

    async def _load_task_states(self, keys: List[str]) -> None:
        if not self.app:
            return
        redis = self.app.get('redis')
        if not redis:
            return
        to_load = [key for key in keys if key not in self._state_loaded_keys]
        if not to_load:
            return
        for key in to_load:
            try:
                payload = await redis.hgetall(f"scheduler:task_state:{key}")
            except Exception as e:
                logger.warning(f"Failed to load task state for {key}: {e}")
                continue
            if not payload:
                try:
                    await redis.srem("scheduler:task_state_keys", key)
                except Exception as e:
                    logger.warning(f"Failed to cleanup task state key {key}: {e}")
                self._state_loaded_keys.add(key)
                continue
            decoded = {k.decode() if isinstance(k, (bytes, bytearray)) else k:
                       v.decode() if isinstance(v, (bytes, bytearray)) else v
                       for k, v in payload.items()}
            state = self._get_task_state(key)
            state["running"] = decoded.get("running") == "1"
            state["last_started_at"] = decoded.get("last_started_at") or None
            state["last_completed_at"] = decoded.get("last_completed_at") or None
            state["last_status"] = decoded.get("last_status") or None
            state["last_success_date"] = decoded.get("last_success_date") or None
            state["last_attempt_date"] = decoded.get("last_attempt_date") or None
            state["last_updated_at"] = decoded.get("last_updated_at") or None
            try:
                state["failure_count"] = int(decoded.get("failure_count") or 0)
            except Exception:
                state["failure_count"] = 0
            self._state_loaded_keys.add(key)

    def _mark_task_running(self, task_name: str, task_id: Optional[str] = None) -> None:
        key = self._state_key(task_name, task_id)
        state = self._get_task_state(key)
        if state.get("running"):
            return
        today = date.today().isoformat()
        if state.get("last_attempt_date") != today:
            state["failure_count"] = 0
        state["running"] = True
        state["last_started_at"] = datetime.utcnow().isoformat()
        state["last_attempt_date"] = today
        state["last_status"] = "running"
        self._schedule_state_persist(key)

    def _mark_task_finished(self, task_name: str, status: str, task_id: Optional[str] = None) -> None:
        key = self._state_key(task_name, task_id)
        state = self._get_task_state(key)
        today = date.today().isoformat()
        if state.get("last_attempt_date") != today:
            state["failure_count"] = 0
            state["last_attempt_date"] = today
        state["running"] = False
        state["last_completed_at"] = datetime.utcnow().isoformat()
        state["last_status"] = status
        if self._is_success_status(status):
            state["last_success_date"] = today
            state["failure_count"] = 0
        elif status == "failed":
            if state.get("last_attempt_date") != today:
                state["failure_count"] = 1
            else:
                state["failure_count"] = int(state.get("failure_count", 0)) + 1
        self._schedule_state_persist(key)

    def _parse_time_str(self, time_str: str) -> dt_time:
        parts = time_str.split(':')
        if len(parts) == 2:
            hour, minute = parts
            second = 0
        else:
            hour, minute, second = parts
        return dt_time(hour=int(hour), minute=int(minute), second=int(second))

    def _parse_cron_spec(self, cron: Optional[str]) -> Dict[str, Any]:
        if not cron or not isinstance(cron, str):
            return {"mode": "every", "seconds": 86400}
        cron = cron.strip().lower()
        if cron.startswith('every:'):
            val = cron.split(':', 1)[1]
            num_str = ''.join(ch for ch in val if ch.isdigit()) or '1'
            unit = ''.join(ch for ch in val if ch.isalpha()) or 'm'
            n = int(num_str)
            if unit in ('s', 'sec', 'secs', 'second', 'seconds'):
                return {"mode": "every", "seconds": max(1, n)}
            if unit in ('m', 'min', 'mins', 'minute', 'minutes'):
                return {"mode": "every", "seconds": max(60, n * 60)}
            if unit in ('h', 'hour', 'hours'):
                return {"mode": "every", "seconds": max(3600, n * 3600)}
            if unit in ('d', 'day', 'days'):
                return {"mode": "every", "seconds": max(86400, n * 86400)}
            return {"mode": "every", "seconds": max(60, n * 60)}
        if cron.startswith('at:'):
            time_str = cron.split(':', 1)[1]
            time_str = time_str if time_str.count(':') == 2 else (time_str + ":00")
            return {"mode": "at", "time": time_str}
        return {"mode": "every", "seconds": 3600}

    def _parse_iso_dt(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except Exception:
            return None

    def _daily_fetch_timeout_seconds(self) -> int:
        """每日行情抓取任务等待超时（秒）"""
        value = getattr(self.config.service, "daily_data_fetch_timeout", None)
        try:
            timeout = int(value) if value is not None else 86400
        except Exception:
            timeout = 86400
        return max(600, timeout)

    def _daily_index_retry_interval_seconds(self) -> int:
        """每日指数抓取失败后的补抓间隔（秒）。"""
        value = getattr(self.config.service, "daily_index_fetch_retry_interval_seconds", None)
        try:
            seconds = int(value) if value is not None else 1800
        except Exception:
            seconds = 1800
        return max(60, seconds)

    def _daily_index_retry_attempts(self) -> int:
        """每日指数抓取失败后的最大重试次数（不含首次触发）。"""
        value = getattr(self.config.service, "daily_index_fetch_retry_attempts", None)
        try:
            attempts = int(value) if value is not None else 3
        except Exception:
            attempts = 3
        return max(0, attempts)

    def _should_trigger(self, state: Dict[str, Any], spec: Dict[str, Any], now: datetime) -> bool:
        today_str = now.date().isoformat()
        try:
            retry_attempts = int(spec.get("retry_attempts", 2))
        except Exception:
            retry_attempts = 2
        retry_attempts = max(0, retry_attempts)
        try:
            retry_interval_seconds = int(spec.get("retry_interval_seconds", 60))
        except Exception:
            retry_interval_seconds = 60
        retry_interval_seconds = max(60, retry_interval_seconds)
        max_failures_today = retry_attempts + 1
        if state.get("running"):
            return False
        if state.get("failure_count", 0) >= max_failures_today and state.get("last_attempt_date") == today_str:
            return False
        if state.get("last_status") == "failed" and state.get("last_attempt_date") == today_str:
            last_completed = self._parse_iso_dt(state.get("last_completed_at"))
            if last_completed and (now - last_completed) < timedelta(seconds=retry_interval_seconds):
                return False
            return True

        mode = spec.get("mode")
        if mode == "at":
            time_str = spec.get("time") or "00:00:00"
            schedule_time = self._parse_time_str(time_str)
            scheduled_dt = datetime.combine(now.date(), schedule_time)
            if now < scheduled_dt:
                return False
            if state.get("last_success_date") == today_str:
                return False
            if state.get("last_attempt_date") == today_str:
                return False
            return True

        interval_seconds = int(spec.get("seconds") or 3600)
        last_ts = self._parse_iso_dt(state.get("last_started_at")) or self._parse_iso_dt(state.get("last_completed_at"))
        if not last_ts:
            return True
        return (now - last_ts) >= timedelta(seconds=interval_seconds)

    def _fire_task(self, task_name: str, task_id: Optional[str], handler: Callable, handler_args: tuple) -> None:
        try:
            self._mark_task_running(task_name, task_id)
            coro = handler(*handler_args)
            task = asyncio.create_task(coro)
            task.add_done_callback(self._log_background_error)
        except Exception as e:
            logger.error(f"Failed to trigger task {task_name}: {e}")

    async def _scheduler_tick(self, context) -> None:
        if self._poll_lock.locked():
            return
        async with self._poll_lock:
            await self._poll_missed_tasks()

    async def _poll_missed_tasks(self) -> None:
        now = datetime.now()
        daily_cron = getattr(self.config.service, 'daily_data_fetch_cron', None)
        daily_spec = self._parse_cron_spec(daily_cron) if daily_cron else {"mode": "every", "seconds": 86400}
        monthly_sw_cron = getattr(self.config.service, 'monthly_sw_industry_full_fetch_cron', None) or "at:03:20"
        monthly_sw_spec = self._parse_cron_spec(monthly_sw_cron)
        strategy_plan_cron = getattr(self.config.service, 'strategy_plan_generation_cron', None) or "at:18:50"
        strategy_plan_spec = self._parse_cron_spec(strategy_plan_cron)

        system_tasks = [
            {
                "task_name": "daily_data_fetch",
                "task_id": None,
                "spec": daily_spec,
                "handler": self.ensure_daily_data_fetched_today,
                "handler_args": ()
            },
            {
                "task_name": "daily_index_fetch",
                "task_id": None,
                "spec": {
                    **daily_spec,
                    "retry_interval_seconds": self._daily_index_retry_interval_seconds(),
                    "retry_attempts": self._daily_index_retry_attempts(),
                },
                "handler": self._trigger_daily_index_fetch,
                "handler_args": (None,)
            },
            {
                "task_name": "daily_market_snapshot_support_fetch",
                "task_id": None,
                "spec": daily_spec,
                "handler": self._trigger_daily_market_snapshot_support_fetch,
                "handler_args": (None,)
            },
            {
                "task_name": "monthly_sw_industry_full_fetch",
                "task_id": None,
                "spec": monthly_sw_spec,
                "handler": self._trigger_monthly_sw_industry_full_fetch,
                "handler_args": (None,)
            },
            {
                "task_name": "daily_board_fetch",
                "task_id": None,
                "spec": daily_spec,
                "handler": self._trigger_daily_board_fetch,
                "handler_args": (None,)
            },
            {
                "task_name": "daily_strategy_plan_generation",
                "task_id": None,
                "spec": strategy_plan_spec,
                "handler": self._trigger_daily_strategy_plan_generation,
                "handler_args": (None,)
            },
            {
                "task_name": "system_health_check",
                "task_id": None,
                "spec": {"mode": "every", "seconds": 1800},
                "handler": self._system_health_check,
                "handler_args": (None,)
            }
        ]

        custom_tasks = []
        for task_id, task in self._managed_tasks.items():
            if not task.get("enabled", True):
                continue
            spec = self._parse_cron_spec(task.get("cron"))
            custom_tasks.append({
                "task_name": task.get("name") or task_id,
                "task_id": task_id,
                "spec": spec,
                "handler": self._execute_custom_task,
                "handler_args": (task,)
            })

        keys = [self._state_key(item["task_name"], item["task_id"]) for item in system_tasks + custom_tasks]
        await self._load_task_states(keys)

        for spec in system_tasks + custom_tasks:
            key = self._state_key(spec["task_name"], spec["task_id"])
            state = self._get_task_state(key)
            if self._should_trigger(state, spec["spec"], now):
                self._fire_task(spec["task_name"], spec["task_id"], spec["handler"], spec["handler_args"])

    async def _restart_scheduler(self):
        """应用任务变更：运行中则热更新新增蓝图，未运行则启动"""
        try:
            self._blueprint = self._create_task_blueprint()
            if self._is_running:
                scheduler = Scheduler.get_instance()
                scheduler.add_plan(self._blueprint)
                self._sync_tasks_from_plans()
                self._refresh_task_id_mapping()
                logger.info("Scheduler updated with new blueprint (hot-add)")
            else:
                start_scheduler([self._blueprint])
                self._is_running = True
                self._refresh_task_id_mapping()
                logger.info("Scheduler started with blueprint")
        except Exception as e:
            logger.error(f"Failed to apply scheduler update: {e}")
            raise AdapterException("IntegrationScheduler", f"Scheduler update failed: {e}")

    async def create_task(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """创建自定义定时任务
        需要字段: name, cron, function(字符串，标识要执行的功能)，可选payload
        """
        name = (data or {}).get('name')
        cron = (data or {}).get('cron')
        func = (data or {}).get('function')
        if not name or not cron or not func:
            raise AdapterException("IntegrationScheduler", "Missing required fields: name/cron/function")
        task_id = str(uuid.uuid4())
        self._managed_tasks[task_id] = {
            'task_id': task_id,
            'name': name,
            'cron': cron,
            'function': func,
            'payload': data.get('payload'),
            'enabled': True,
            'created_at': datetime.utcnow().isoformat()
        }
        await self._restart_scheduler()
        return self._managed_tasks[task_id]

    async def update_task(self, task_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """更新自定义任务配置并应用"""
        if task_id not in self._managed_tasks:
            raise AdapterException("IntegrationScheduler", f"Task not found: {task_id}")
        t = self._managed_tasks[task_id]
        for k in ('name', 'cron', 'function', 'payload', 'enabled'):
            if k in data:
                t[k] = data[k]
        t['updated_at'] = datetime.utcnow().isoformat()
        await self._restart_scheduler()
        return t

    async def delete_task(self, task_id: str) -> bool:
        """删除自定义任务：从注册表移除并重启调度器"""
        if task_id not in self._managed_tasks:
            raise AdapterException("IntegrationScheduler", f"Task not found: {task_id}")
        self._managed_tasks.pop(task_id, None)
        await self._restart_scheduler()
        return True

    async def toggle_task(self, task_id: str) -> Dict[str, Any]:
        """切换任务启用状态并应用"""
        if task_id not in self._managed_tasks:
            raise AdapterException("IntegrationScheduler", f"Task not found: {task_id}")
        t = self._managed_tasks[task_id]
        t['enabled'] = not t.get('enabled', True)
        t['updated_at'] = datetime.utcnow().isoformat()
        await self._restart_scheduler()
        return t



    async def get_all_tasks(self) -> List[Dict[str, Any]]:
        """获取所有任务"""
        if not self._is_running:
            return []

        try:
            scheduler = Scheduler.get_instance()
            tasks = scheduler.list_tasks()
            results = []
            seen_custom = set()
            for task in tasks:
                scheduler_task_id = task.get_task_id()
                task_name = task.get_task_name()
                if task_name.startswith("/custom/"):
                    task_id = task_name[len("/custom/"):]
                    managed = self._managed_tasks.get(task_id)
                    if managed:
                        managed['scheduler_task_id'] = scheduler_task_id
                        seen_custom.add(task_id)
                        results.append({
                            'name': managed.get('name') or task_name,
                            'task_id': task_id,
                            'status': 'active' if self._is_running else 'stopped',
                            'cron': managed.get('cron'),
                            'function': managed.get('function'),
                            'enabled': managed.get('enabled', True),
                            'source': 'custom'
                        })
                        continue
                if task_name.startswith("/custom/"):
                    results.append({
                        'name': task_name,
                        'task_id': scheduler_task_id,
                        'status': 'active' if self._is_running else 'stopped',
                        'cron': None,
                        'function': None,
                        'enabled': True,
                        'source': 'custom'
                    })
                else:
                    results.append({
                        'name': task_name,
                        'task_id': scheduler_task_id,
                        'status': 'active' if self._is_running else 'stopped',
                        'cron': None,
                        'function': None,
                        'enabled': True,
                        'source': 'system'
                    })
            for task_id, managed in self._managed_tasks.items():
                if task_id in seen_custom:
                    continue
                results.append({
                    'name': managed.get('name') or self._custom_task_id(task_id),
                    'task_id': task_id,
                    'status': 'active' if self._is_running else 'stopped',
                    'cron': managed.get('cron'),
                    'function': managed.get('function'),
                    'enabled': managed.get('enabled', True),
                    'source': 'custom'
                })
            return results
        except Exception as e:
            logger.error(f"Failed to get tasks: {e}")
            return []

    async def get_task(self, task_id: str) -> Dict[str, Any]:
        """获取任务详情"""
        try:
            if task_id in self._managed_tasks:
                managed = self._managed_tasks[task_id]
                return {
                    'task_id': task_id,
                    'name': managed.get('name'),
                    'status': 'active' if self._is_running else 'stopped',
                    'cron': managed.get('cron'),
                    'function': managed.get('function'),
                    'enabled': managed.get('enabled', True),
                    'payload': managed.get('payload'),
                    'source': 'custom'
                }
            scheduler = Scheduler.get_instance()
            task = scheduler.get_task(task_id)
            return {
                'task_id': task_id,
                'name': task.get_task_name(),
                'status': 'active' if self._is_running else 'stopped'
            }
        except Exception as e:
            raise AdapterException("IntegrationScheduler", f"Task not found: {task_id}")

    async def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """获取任务运行状态（供 jobs/status 使用）"""
        task_name = task_id
        try:
            task = await self.get_task(task_id)
            task_name = task.get('name') or task_id
            # 自定义任务用 task_id 作为状态键
            if task.get('source') == 'custom':
                task_name = task_id
        except Exception:
            # 任务不存在时仍返回 unknown
            return {
                'job_id': task_id,
                'status': 'unknown',
                'progress': 0
            }

        state_key = self._state_key(task_name)
        state = self._get_task_state(state_key)
        status = 'running' if state.get('running') else (state.get('last_status') or 'idle')
        progress = 100 if self._is_success_status(status) else 0
        if status in {"failed", "cancelled", "canceled"}:
            progress = 0

        message = None
        if self._task_history:
            for record in reversed(self._task_history):
                if record.get('task_id') == task_id or record.get('task_name') == task_name:
                    message = record.get('message')
                    break

        return {
            'job_id': task_id,
            'status': status,
            'progress': progress,
            'started_at': state.get('last_started_at'),
            'completed_at': state.get('last_completed_at'),
            'updated_at': state.get('last_updated_at'),
            'message': message
        }

    async def trigger_task(self, task_id: str) -> Dict[str, Any]:
        """手动触发任务"""
        try:
            scheduler = Scheduler.get_instance()
            if task_id in self._managed_tasks:
                task_data = self._managed_tasks.get(task_id, {})
                background = asyncio.create_task(self._execute_custom_task(task_data))
                background.add_done_callback(self._log_background_error)
            else:
                scheduler.start_task(task_id)
            return {
                'task_id': task_id,
                'status': 'triggered',
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to trigger task {task_id}: {e}")
            raise AdapterException("IntegrationScheduler", f"Task trigger failed: {e}")

    @staticmethod
    def _log_background_error(task: asyncio.Task) -> None:
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        if exc:
            logger.error(f"Background task execution failed: {exc}")

    # 默认任务实现
    async def _trigger_daily_data_fetch(self, context):
        """触发每日数据抓取

        注意：
        1. 创建 Flowhub 数据抓取任务
        2. 等待任务完成（超时可配置）
        3. 通知 AnalysisTriggerScheduler 各个数据抓取任务已完成
        """
        flowhub_adapter = None
        self._mark_task_running("daily_data_fetch")
        try:
            logger.info("Triggering daily data fetch...")

            # 获取FlowhubAdapter实例
            flowhub_adapter = await self._get_flowhub_adapter()

            if flowhub_adapter:
                task = await flowhub_adapter.ensure_task(
                    name="brain_daily_data_fetch",
                    data_type="batch_daily_ohlc",
                    params={
                        "data_type": "batch_daily_ohlc",
                        "symbols": None,
                        "incremental": True,
                        "force_update": False
                    }
                )
                run_result = await flowhub_adapter.run_task(task.get('task_id'))
                job_id = run_result.get('job_id')
                logger.info(f"Daily data fetch job created: {job_id}")

                timeout_seconds = self._daily_fetch_timeout_seconds()
                logger.info(f"Waiting for job {job_id} to complete (timeout: {timeout_seconds}s)...")
                result = await flowhub_adapter.wait_for_job_completion(job_id, timeout=timeout_seconds)

                job_status = result.get('status')
                if self._is_success_status(job_status):
                    logger.info(f"Daily data fetch job {job_id} completed successfully")

                    # 记录任务执行成功
                    self._record_task_execution(
                        "daily_data_fetch",
                        "completed",
                        f"Daily data fetch job completed: {job_id}"
                    )

                    # 通知 AnalysisTriggerScheduler 数据抓取任务已完成
                    await self._notify_data_fetch_completed(['stock_daily_data_fetch'])

                else:
                    logger.warning(f"Daily data fetch job {job_id} finished with status: {job_status}")
                    self._record_task_execution(
                        "daily_data_fetch",
                        "failed",
                        f"Job finished with status: {job_status}"
                    )

            else:
                logger.warning("FlowhubAdapter not available, skipping data fetch")
                self._record_task_execution("daily_data_fetch", "skipped", "FlowhubAdapter not available")

        except Exception as e:
            logger.error(f"Daily data fetch failed: {e}", exc_info=True)
            self._record_task_execution("daily_data_fetch", "failed", str(e))
        finally:
            if flowhub_adapter:
                try:
                    await flowhub_adapter.disconnect_from_system()
                except Exception as ce:
                    logger.warning(f"Failed to close FlowhubAdapter session: {ce}")

    async def ensure_daily_data_fetched_today(self):
        """启动时检查当天数据抓取是否完成，必要时触发补抓"""
        flowhub_adapter = None
        self._mark_task_running("daily_data_fetch")
        try:
            logger.info("Checking daily data fetch status for today...")
            flowhub_adapter = await self._get_flowhub_adapter()
            if not flowhub_adapter:
                logger.warning("FlowhubAdapter not available, scheduling retry for daily data fetch check")
                self._schedule_daily_fetch_retry()
                self._record_task_execution(
                    "daily_data_fetch",
                    "failed",
                    "FlowhubAdapter not available"
                )
                return
            self._reset_daily_fetch_retry()

            task = await flowhub_adapter.ensure_task(
                name="brain_daily_data_fetch",
                data_type="batch_daily_ohlc",
                params={
                    "data_type": "batch_daily_ohlc",
                    "symbols": None,
                    "incremental": True,
                    "force_update": False
                }
            )
            last_run_at = task.get("last_run_at")
            last_status = task.get("last_status") or task.get("status")
            today = datetime.now().date()
            if last_run_at:
                try:
                    last_run_date = datetime.fromtimestamp(float(last_run_at)).date()
                except Exception:
                    last_run_date = None
                if last_run_date == today:
                    if self._is_success_status(last_status):
                        logger.info("Daily data fetch already completed for today, skip trigger")
                        job_id = task.get("last_job_id") or task.get("current_job_id")
                        if job_id:
                            try:
                                status_payload = await flowhub_adapter.get_job_status(job_id)
                                result = status_payload.get("result") or {}
                                failed_count = int(result.get("failed_count") or 0)
                                if failed_count > 0:
                                    logger.warning(f"Daily data fetch had {failed_count} failed symbols, retrying once")
                                    retry_result = await self._retry_daily_fetch_once(flowhub_adapter, task)
                                    if retry_result and self._is_success_status(retry_result.get("status")):
                                        logger.info("Daily data fetch retry completed successfully")
                                        self._record_task_execution(
                                            "daily_data_fetch",
                                            "completed",
                                            f"Daily data fetch job completed: {retry_result.get('job_id')}"
                                        )
                                        await self._notify_data_fetch_completed(['stock_daily_data_fetch'])
                                        return
                                    self._record_task_execution(
                                        "daily_data_fetch",
                                        "failed",
                                        f"Daily data fetch retry failed after {failed_count} failures"
                                    )
                                    return
                            except Exception as status_err:
                                logger.warning(f"Failed to inspect daily fetch result: {status_err}")
                        self._record_task_execution(
                            "daily_data_fetch",
                            "completed",
                            "Daily data fetch already completed for today"
                        )
                        await self._notify_data_fetch_completed(['stock_daily_data_fetch'])
                        return
                    if last_status in {"running", "queued"}:
                        job_id = task.get("current_job_id") or task.get("last_job_id")
                        if not job_id:
                            logger.info("Daily data fetch running for today but no job id, skip trigger")
                            self._record_task_execution(
                                "daily_data_fetch",
                                "skipped",
                                "Daily data fetch running but no job id"
                            )
                            return
                        stale_threshold = 300
                        try:
                            status_payload = await flowhub_adapter.get_job_status(job_id)
                        except Exception as status_err:
                            logger.warning(f"Failed to fetch job status for {job_id}: {status_err}")
                            status_payload = None
                        updated_at = None
                        status_value = None
                        if isinstance(status_payload, dict):
                            updated_at = status_payload.get("updated_at")
                            status_value = status_payload.get("status")
                        if status_value in {"failed", "cancelled", "canceled"}:
                            logger.warning(f"Daily data fetch job {job_id} reported {status_value}, re-triggering task")
                            try:
                                run_result = await flowhub_adapter.run_task(task.get("task_id"))
                                job_id = run_result.get("job_id") or job_id
                            except Exception as rerun_err:
                                logger.warning(f"Failed to re-run daily data fetch task: {rerun_err}")
                        elif status_value != "running":
                            running_job_id = await self._resolve_running_daily_job(flowhub_adapter)
                            if running_job_id:
                                job_id = running_job_id
                                logger.info(f"Switching to running batch_daily_ohlc job {job_id} (current status={status_value})")
                        is_stale = updated_at is None or (time.time() - float(updated_at)) > stale_threshold
                        if is_stale:
                            logger.warning(f"Daily data fetch job {job_id} appears stale, restarting task")
                            try:
                                await flowhub_adapter.cancel_job(job_id)
                            except Exception as cancel_err:
                                logger.warning(f"Failed to cancel stale job {job_id}: {cancel_err}")
                            try:
                                run_result = await flowhub_adapter.run_task(task.get("task_id"))
                                job_id = run_result.get("job_id") or job_id
                                logger.info(f"Started new daily data fetch job {job_id} after stale detection")
                            except Exception as rerun_err:
                                logger.warning(f"Failed to re-run daily data fetch task after stale detection: {rerun_err}")
                                running_job_id = await self._resolve_running_daily_job(flowhub_adapter)
                                if running_job_id:
                                    job_id = running_job_id
                                    logger.info(f"Falling back to running batch_daily_ohlc job {job_id} for daily data fetch")
                        logger.info(f"Daily data fetch already running for today, waiting for job {job_id} to complete...")
                        timeout_seconds = self._daily_fetch_timeout_seconds()
                        result = await flowhub_adapter.wait_for_job_completion(job_id, timeout=timeout_seconds)
                        job_status = result.get("status")
                        if self._is_success_status(job_status):
                            logger.info(f"Daily data fetch job {job_id} completed successfully")
                            self._record_task_execution(
                                "daily_data_fetch",
                                "completed",
                                f"Daily data fetch job completed: {job_id}"
                            )
                            await self._notify_data_fetch_completed(['stock_daily_data_fetch'])
                        else:
                            logger.warning(f"Daily data fetch job {job_id} finished with status: {job_status}")
                            retry_result = await self._retry_daily_fetch_once(flowhub_adapter, task)
                            if retry_result and self._is_success_status(retry_result.get("status")):
                                logger.info("Daily data fetch retry completed successfully")
                                self._record_task_execution(
                                    "daily_data_fetch",
                                    "completed",
                                    f"Daily data fetch job completed: {retry_result.get('job_id')}"
                                )
                                await self._notify_data_fetch_completed(['stock_daily_data_fetch'])
                            else:
                                self._record_task_execution(
                                    "daily_data_fetch",
                                    "failed",
                                    f"Job finished with status: {job_status}"
                                )
                        return

            recent_job = await self._find_recent_completed_daily_job(flowhub_adapter, today)
            if recent_job:
                job_id = recent_job.get("job_id")
                logger.info(f"Found completed batch_daily_ohlc job {job_id} for today, skip trigger")
                self._record_task_execution(
                    "daily_data_fetch",
                    "completed",
                    f"Daily data fetch job completed: {job_id}"
                )
                await self._notify_data_fetch_completed(['stock_daily_data_fetch'])
                return

            logger.info("Daily data fetch not completed today, triggering...")
            run_result = await flowhub_adapter.run_task(task.get("task_id"))
            job_id = run_result.get("job_id")
            if not job_id:
                raise AdapterException("IntegrationScheduler", "Failed to create daily data fetch job")
            running_job_id = await self._resolve_running_daily_job(flowhub_adapter)
            if running_job_id and running_job_id != job_id:
                logger.info(f"Switching to running batch_daily_ohlc job {running_job_id} after trigger")
                job_id = running_job_id

            timeout_seconds = self._daily_fetch_timeout_seconds()
            result = await flowhub_adapter.wait_for_job_completion(job_id, timeout=timeout_seconds)
            job_status = result.get("status")
            if self._is_success_status(job_status):
                logger.info(f"Daily data fetch job {job_id} completed successfully")
                self._record_task_execution(
                    "daily_data_fetch",
                    "completed",
                    f"Daily data fetch job completed: {job_id}"
                )
                await self._notify_data_fetch_completed(['stock_daily_data_fetch'])
            else:
                logger.warning(f"Daily data fetch job {job_id} finished with status: {job_status}")
                retry_result = await self._retry_daily_fetch_once(flowhub_adapter, task)
                if retry_result and self._is_success_status(retry_result.get("status")):
                    logger.info("Daily data fetch retry completed successfully")
                    self._record_task_execution(
                        "daily_data_fetch",
                        "completed",
                        f"Daily data fetch job completed: {retry_result.get('job_id')}"
                    )
                    await self._notify_data_fetch_completed(['stock_daily_data_fetch'])
                else:
                    self._record_task_execution(
                        "daily_data_fetch",
                        "failed",
                        f"Job finished with status: {job_status}"
                    )

        except Exception as e:
            logger.error(f"Daily data fetch check failed: {e}", exc_info=True)
            self._record_task_execution("daily_data_fetch", "failed", str(e))
            self._schedule_daily_fetch_retry()
        finally:
            if flowhub_adapter:
                try:
                    await flowhub_adapter.disconnect_from_system()
                except Exception as ce:
                    logger.warning(f"Failed to close FlowhubAdapter session: {ce}")

    def _schedule_daily_fetch_retry(self, delay_seconds: int = 10, max_retries: int = 5) -> None:
        """Flowhub 未就绪时延迟重试 daily fetch check"""
        if self._daily_fetch_retry_count >= max_retries:
            logger.warning("Daily data fetch retry limit reached, stop retrying")
            return
        if self._daily_fetch_retry_task and not self._daily_fetch_retry_task.done():
            return
        self._daily_fetch_retry_count += 1

        async def _retry():
            await asyncio.sleep(delay_seconds)
            await self.ensure_daily_data_fetched_today()

        self._daily_fetch_retry_task = asyncio.create_task(_retry())
        logger.info(f"Scheduled daily data fetch retry #{self._daily_fetch_retry_count} in {delay_seconds}s")

    def _reset_daily_fetch_retry(self) -> None:
        """Flowhub 可用后重置重试状态"""
        self._daily_fetch_retry_count = 0
        if self._daily_fetch_retry_task and not self._daily_fetch_retry_task.done():
            self._daily_fetch_retry_task.cancel()
        self._daily_fetch_retry_task = None

    async def _resolve_running_daily_job(self, flowhub_adapter) -> Optional[str]:
        """优先选择正在运行的 batch_daily_ohlc 任务，避免等待排队任务"""
        try:
            running_jobs = await flowhub_adapter.list_jobs(status='running', limit=50, offset=0)
        except Exception as list_err:
            logger.warning(f"Failed to list running jobs: {list_err}")
            return None

        for job in running_jobs:
            params = job.get("params") or {}
            if params.get("data_type") == "batch_daily_ohlc":
                task_name = params.get("task_name")
                if task_name in (None, "brain_daily_data_fetch"):
                    return job.get("job_id")
        return None

    async def _find_recent_completed_daily_job(self, flowhub_adapter, target_date: date) -> Optional[Dict[str, Any]]:
        """查找当天已完成的 batch_daily_ohlc 任务，避免重复排队"""
        try:
            completed_jobs = await flowhub_adapter.list_jobs(status='succeeded', limit=50, offset=0)
        except Exception as list_err:
            logger.warning(f"Failed to list completed jobs: {list_err}")
            return None

        for job in completed_jobs:
            if job.get("job_type") != "batch_daily_ohlc":
                continue
            ts = job.get("updated_at") or job.get("created_at")
            if not ts:
                continue
            try:
                job_date = datetime.fromtimestamp(float(ts)).date()
            except Exception:
                continue
            if job_date == target_date:
                return job
        return None

    async def _retry_daily_fetch_once(self, flowhub_adapter, task: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """失败后重试一次每日数据抓取任务"""
        try:
            run_result = await flowhub_adapter.run_task(task.get("task_id"))
            job_id = run_result.get("job_id")
            if not job_id:
                return None
            logger.info(f"Retrying daily data fetch with job {job_id}")
            timeout_seconds = self._daily_fetch_timeout_seconds()
            return await flowhub_adapter.wait_for_job_completion(job_id, timeout=timeout_seconds)
        except Exception as retry_err:
            logger.warning(f"Daily data fetch retry failed: {retry_err}")
            return None

    async def _trigger_daily_index_fetch(self, context):
        """触发每日指数数据抓取"""
        flowhub_adapter = None
        self._mark_task_running("daily_index_fetch")
        try:
            logger.info("Triggering daily index data fetch...")

            # 获取FlowhubAdapter实例
            flowhub_adapter = await self._get_flowhub_adapter()

            if flowhub_adapter:
                task = await flowhub_adapter.ensure_task(
                    name="brain_daily_index_fetch",
                    data_type="index_daily_data",
                    params={
                        "data_type": "index_daily_data",
                        "index_codes": None,
                        "update_mode": "incremental"
                    }
                )
                run_result = await flowhub_adapter.run_task(task.get('task_id'))
                job_id = run_result.get('job_id')
                if not job_id:
                    raise AdapterException("IntegrationScheduler", "Failed to create daily index data fetch job")
                logger.info(f"Index daily data fetch job created: {job_id}")

                # 等待任务完成（最多30分钟）
                logger.info(f"Waiting for job {job_id} to complete (timeout: 30 minutes)...")
                result = await flowhub_adapter.wait_for_job_completion(job_id, timeout=1800)

                job_status = result.get('status')
                result_payload = result.get("result") if isinstance(result, dict) and isinstance(result.get("result"), dict) else {}
                try:
                    records_count = int(result_payload.get("records_count") or 0)
                except Exception:
                    records_count = 0
                try:
                    saved_count = int(result_payload.get("saved_count") or 0)
                except Exception:
                    saved_count = 0
                try:
                    skipped_count = int(result_payload.get("skipped_count") or 0)
                except Exception:
                    skipped_count = 0

                has_any_result = (records_count > 0) or (saved_count > 0) or (skipped_count > 0)
                if self._is_success_status(job_status) and has_any_result:
                    logger.info(f"Index daily data fetch job {job_id} completed successfully")
                    self._record_task_execution(
                        "daily_index_fetch",
                        "completed",
                        (
                            "Index data fetch job completed: "
                            f"{job_id} (records={records_count}, saved={saved_count}, skipped={skipped_count})"
                        )
                    )
                    await self._notify_data_fetch_completed(['stock_index_data_fetch'])
                elif self._is_success_status(job_status):
                    logger.warning(
                        "Index daily data fetch returned empty dataset, will trigger retry policy",
                        extra={
                            "job_id": job_id,
                            "records_count": records_count,
                            "saved_count": saved_count,
                            "skipped_count": skipped_count,
                        },
                    )
                    self._record_task_execution(
                        "daily_index_fetch",
                        "failed",
                        (
                            "Index data fetch returned empty dataset "
                            f"(job_id={job_id}, records={records_count}, saved={saved_count}, skipped={skipped_count})"
                        )
                    )
                else:
                    logger.warning(f"Index daily data fetch job {job_id} finished with status: {job_status}")
                    self._record_task_execution(
                        "daily_index_fetch",
                        "failed",
                        f"Job finished with status: {job_status}"
                    )
            else:
                logger.warning("FlowhubAdapter not available, skipping index data fetch")
                self._record_task_execution("daily_index_fetch", "skipped", "FlowhubAdapter not available")

        except Exception as e:
            logger.error(f"Daily index data fetch failed: {e}", exc_info=True)
            self._record_task_execution("daily_index_fetch", "failed", str(e))
        finally:
            if flowhub_adapter:
                try:
                    await flowhub_adapter.disconnect_from_system()
                except Exception as ce:
                    logger.warning(f"Failed to close FlowhubAdapter session: {ce}")

    async def _trigger_daily_market_snapshot_support_fetch(self, context):
        """触发市场快照支撑数据抓取（交易日历、停复牌、申万行业映射）。"""
        flowhub_adapter = None
        self._mark_task_running("daily_market_snapshot_support_fetch")
        try:
            logger.info("Triggering market snapshot support data fetch...")
            flowhub_adapter = await self._get_flowhub_adapter()
            if not flowhub_adapter:
                logger.warning("FlowhubAdapter not available, skipping market snapshot support fetch")
                self._record_task_execution(
                    "daily_market_snapshot_support_fetch",
                    "skipped",
                    "FlowhubAdapter not available"
                )
                return

            task_specs = [
                (
                    "brain_trade_calendar_fetch",
                    "trade_calendar_data",
                    {
                        "data_type": "trade_calendar_data",
                        "exchange": "SSE",
                        "update_mode": "incremental",
                    },
                ),
                (
                    "brain_macro_calendar_fetch",
                    "macro_calendar_data",
                    {
                        "data_type": "macro_calendar_data",
                        "incremental": True,
                    },
                ),
                (
                    "brain_suspend_data_fetch",
                    "suspend_data",
                    {
                        "data_type": "suspend_data",
                        "update_mode": "incremental",
                    },
                ),
                (
                    "brain_sw_industry_fetch",
                    "sw_industry_data",
                    {
                        "data_type": "sw_industry_data",
                        "src": "SW2021",
                        "include_members": True,
                        "update_mode": "incremental",
                    },
                ),
            ]

            job_results: List[tuple[str, Optional[str], str]] = []
            for task_name, data_type, params in task_specs:
                task = await flowhub_adapter.ensure_task(
                    name=task_name,
                    data_type=data_type,
                    params=params,
                )
                run_result = await flowhub_adapter.run_task(task.get("task_id"))
                job_id = run_result.get("job_id")
                if not job_id:
                    job_results.append((task_name, None, "failed"))
                    continue
                logger.info(f"{task_name} job created: {job_id}")
                result = await flowhub_adapter.wait_for_job_completion(job_id, timeout=1800)
                status = str(result.get("status") or "unknown")
                job_results.append((task_name, job_id, status))

            failed_jobs = [item for item in job_results if not self._is_success_status(item[2])]
            if failed_jobs:
                details = ", ".join(f"{name}:{status}" for name, _, status in failed_jobs)
                self._record_task_execution(
                    "daily_market_snapshot_support_fetch",
                    "partial",
                    f"Support data fetch partially failed ({details})"
                )
                return

            details = ", ".join(f"{name}:{job_id}" for name, job_id, _ in job_results if job_id)
            self._record_task_execution(
                "daily_market_snapshot_support_fetch",
                "completed",
                f"Support data fetch completed ({details})"
            )

        except Exception as e:
            logger.error(f"Market snapshot support data fetch failed: {e}", exc_info=True)
            self._record_task_execution("daily_market_snapshot_support_fetch", "failed", str(e))
        finally:
            if flowhub_adapter:
                try:
                    await flowhub_adapter.disconnect_from_system()
                except Exception as ce:
                    logger.warning(f"Failed to close FlowhubAdapter session: {ce}")

    async def _trigger_monthly_sw_industry_full_fetch(self, context):
        """触发申万行业映射月度全量校验（每月1日执行）。"""
        flowhub_adapter = None
        self._mark_task_running("monthly_sw_industry_full_fetch")
        try:
            today = date.today()
            if today.day != 1:
                self._record_task_execution(
                    "monthly_sw_industry_full_fetch",
                    "skipped",
                    f"Skip full update on non-first day ({today.isoformat()})"
                )
                return

            logger.info("Triggering monthly sw industry full fetch...")
            flowhub_adapter = await self._get_flowhub_adapter()
            if not flowhub_adapter:
                logger.warning("FlowhubAdapter not available, skipping monthly sw industry full fetch")
                self._record_task_execution(
                    "monthly_sw_industry_full_fetch",
                    "skipped",
                    "FlowhubAdapter not available"
                )
                return

            task = await flowhub_adapter.ensure_task(
                name="brain_sw_industry_monthly_full_fetch",
                data_type="sw_industry_data",
                params={
                    "data_type": "sw_industry_data",
                    "src": "SW2021",
                    "include_members": True,
                    "update_mode": "full_update",
                },
            )
            run_result = await flowhub_adapter.run_task(task.get("task_id"))
            job_id = run_result.get("job_id")
            if not job_id:
                self._record_task_execution(
                    "monthly_sw_industry_full_fetch",
                    "failed",
                    "No job_id returned from flowhub task run"
                )
                return

            logger.info(f"Monthly sw industry full fetch job created: {job_id}")
            result = await flowhub_adapter.wait_for_job_completion(job_id, timeout=3600)
            job_status = str(result.get("status") or "unknown")
            if self._is_success_status(job_status):
                self._record_task_execution(
                    "monthly_sw_industry_full_fetch",
                    "completed",
                    f"SW industry full update completed (job_id={job_id})"
                )
            else:
                self._record_task_execution(
                    "monthly_sw_industry_full_fetch",
                    "failed",
                    f"Job finished with status: {job_status} (job_id={job_id})"
                )
        except Exception as e:
            logger.error(f"Monthly sw industry full fetch failed: {e}", exc_info=True)
            self._record_task_execution("monthly_sw_industry_full_fetch", "failed", str(e))
        finally:
            if flowhub_adapter:
                try:
                    await flowhub_adapter.disconnect_from_system()
                except Exception as ce:
                    logger.warning(f"Failed to close FlowhubAdapter session: {ce}")

    async def _trigger_daily_board_fetch(self, context):
        """触发每日板块数据抓取（板块行情 + 成分股 + 日频基础数据）"""
        flowhub_adapter = None
        self._mark_task_running("daily_board_fetch")
        try:
            logger.info("Triggering daily board data fetch...")

            # 获取FlowhubAdapter实例
            flowhub_adapter = await self._get_flowhub_adapter()

            if flowhub_adapter:
                task_specs = [
                    {
                        "name": "brain_industry_board_fetch",
                        "data_type": "industry_board",
                        "params": {
                            "data_type": "industry_board",
                            "source": "ths",
                            "update_mode": "incremental",
                        },
                        "notify_key": "industry_board_data_fetch",
                        "schedule_type": "cron",
                        "schedule_value": self.STRUCTURE_FLOWHUB_CRON_TEMPLATES["brain_industry_board_fetch"],
                        "timeout": 14400,
                    },
                    {
                        "name": "brain_concept_board_fetch",
                        "data_type": "concept_board",
                        "params": {
                            "data_type": "concept_board",
                            "source": "ths",
                            "update_mode": "incremental",
                        },
                        "notify_key": "concept_board_data_fetch",
                        "schedule_type": "cron",
                        "schedule_value": self.STRUCTURE_FLOWHUB_CRON_TEMPLATES["brain_concept_board_fetch"],
                        "timeout": 14400,
                    },
                    {
                        "name": "brain_industry_board_stocks_fetch",
                        "data_type": "industry_board_stocks",
                        "params": {
                            "data_type": "industry_board_stocks",
                            "source": "ths",
                            "update_mode": "incremental",
                        },
                        "notify_key": "industry_board_stocks_data_fetch",
                        "schedule_type": "cron",
                        "schedule_value": self.STRUCTURE_FLOWHUB_CRON_TEMPLATES["brain_industry_board_stocks_fetch"],
                        "timeout": 21600,
                    },
                    {
                        "name": "brain_concept_board_stocks_fetch",
                        "data_type": "concept_board_stocks",
                        "params": {
                            "data_type": "concept_board_stocks",
                            "source": "ths",
                            "update_mode": "incremental",
                        },
                        "notify_key": "concept_board_stocks_data_fetch",
                        "schedule_type": "cron",
                        "schedule_value": self.STRUCTURE_FLOWHUB_CRON_TEMPLATES["brain_concept_board_stocks_fetch"],
                        "timeout": 21600,
                    },
                    {
                        "name": "brain_industry_moneyflow_fetch",
                        "data_type": "industry_moneyflow_data",
                        "params": {
                            "data_type": "industry_moneyflow_data",
                            "source": "ths",
                            "incremental": True,
                        },
                        "notify_key": "industry_moneyflow_data_fetch",
                        "schedule_type": "cron",
                        "schedule_value": self.STRUCTURE_FLOWHUB_CRON_TEMPLATES["brain_industry_moneyflow_fetch"],
                        "timeout": 21600,
                    },
                    {
                        "name": "brain_concept_moneyflow_fetch",
                        "data_type": "concept_moneyflow_data",
                        "params": {
                            "data_type": "concept_moneyflow_data",
                            "source": "em",
                            "incremental": True,
                        },
                        "notify_key": "concept_moneyflow_data_fetch",
                        "schedule_type": "cron",
                        "schedule_value": self.STRUCTURE_FLOWHUB_CRON_TEMPLATES["brain_concept_moneyflow_fetch"],
                        "timeout": 21600,
                    },
                    {
                        "name": "brain_batch_daily_basic_fetch",
                        "data_type": "batch_daily_basic",
                        "params": {
                            "data_type": "batch_daily_basic",
                            "incremental": True,
                        },
                        "notify_key": "batch_daily_basic_data_fetch",
                        "schedule_type": "cron",
                        "schedule_value": self.STRUCTURE_FLOWHUB_CRON_TEMPLATES["brain_batch_daily_basic_fetch"],
                        "timeout": 21600,
                    },
                ]

                running_jobs: List[Dict[str, Any]] = []
                for spec in task_specs:
                    task = await flowhub_adapter.ensure_task(
                        name=spec["name"],
                        data_type=spec["data_type"],
                        params=spec["params"],
                        schedule_type=spec.get("schedule_type"),
                        schedule_value=spec.get("schedule_value"),
                        enabled=True,
                        allow_overlap=False,
                    )
                    run_result = await flowhub_adapter.run_task(task.get("task_id"))
                    job_id = run_result.get("job_id")
                    logger.info(f"{spec['data_type']} fetch job created: {job_id}")
                    if not job_id:
                        running_jobs.append(
                            {
                                "data_type": spec["data_type"],
                                "notify_key": spec["notify_key"],
                                "job_id": "",
                                "timeout": int(spec.get("timeout") or 14400),
                                "status": "failed",
                            }
                        )
                        continue
                    running_jobs.append(
                        {
                            "data_type": spec["data_type"],
                            "notify_key": spec["notify_key"],
                            "job_id": job_id,
                            "timeout": int(spec.get("timeout") or 14400),
                        }
                    )

                completed: List[Dict[str, Any]] = []
                for job in running_jobs:
                    if not job.get("job_id"):
                        completed.append(job)
                        continue
                    logger.info(f"Waiting for {job['data_type']} job {job['job_id']} to complete...")
                    result = await flowhub_adapter.wait_for_job_completion(job["job_id"], timeout=job["timeout"])
                    status = str(result.get("status") or "unknown")
                    completed.append({**job, "status": status})

                success_jobs = [job for job in completed if self._is_success_status(job.get("status"))]
                failed_jobs = [job for job in completed if not self._is_success_status(job.get("status"))]

                if not failed_jobs:
                    logger.info("Board/structure data fetch jobs completed successfully")
                    self._record_task_execution(
                        "daily_board_fetch",
                        "completed",
                        ", ".join(f"{job['data_type']}:{job['job_id']}" for job in completed),
                    )
                else:
                    logger.warning(
                        "Board/structure data fetch jobs finished with partial failures: "
                        + ", ".join(f"{job['data_type']}={job['status']}" for job in completed)
                    )
                    self._record_task_execution(
                        "daily_board_fetch",
                        "partial",
                        ", ".join(f"{job['data_type']}={job['status']}" for job in completed),
                    )

                tasks_to_notify = [job["notify_key"] for job in success_jobs if job.get("notify_key")]
                if tasks_to_notify:
                    await self._notify_data_fetch_completed(tasks_to_notify)
            else:
                logger.warning("FlowhubAdapter not available, skipping board data fetch")
                self._record_task_execution("daily_board_fetch", "skipped", "FlowhubAdapter not available")

        except Exception as e:
            logger.error(f"Daily board data fetch failed: {e}", exc_info=True)
            self._record_task_execution("daily_board_fetch", "failed", str(e))
        finally:
            if flowhub_adapter:
                try:
                    await flowhub_adapter.disconnect_from_system()
                except Exception as ce:
                    logger.warning(f"Failed to close FlowhubAdapter session: {ce}")

    def _strategy_plan_timeout_seconds(self) -> int:
        value = getattr(self.config.service, "strategy_plan_generation_timeout", None)
        try:
            timeout = int(value) if value is not None else 7200
        except Exception:
            timeout = 7200
        return max(300, timeout)

    def _build_strategy_plan_job_params(self, today: date) -> Dict[str, Any]:
        lookback_days_raw = getattr(self.config.service, "strategy_plan_lookback_days", 180)
        try:
            lookback_days = int(lookback_days_raw)
        except Exception:
            lookback_days = 180
        lookback_days = max(30, lookback_days)

        initial_cash_raw = getattr(self.config.service, "strategy_plan_initial_cash", 3000000.0)
        try:
            initial_cash = float(initial_cash_raw)
        except Exception:
            initial_cash = 3000000.0
        initial_cash = max(1000.0, initial_cash)

        benchmark = str(getattr(self.config.service, "strategy_plan_benchmark", "000300.SH") or "000300.SH").strip() or "000300.SH"
        cost = str(getattr(self.config.service, "strategy_plan_cost", "5bp") or "5bp").strip() or "5bp"
        start_day = today - timedelta(days=lookback_days)
        return {
            "title": f"AutoPlan-{today.isoformat()}",
            "subject_id": None,
            "config": {
                "mode": "backtest",
                "universe": "portfolio_universe",
                "strategy": "policy_chain_v1",
                "from": start_day.isoformat(),
                "to": today.isoformat(),
                "freq": "daily",
                "fill": "next_open",
                "cash": initial_cash,
                "benchmark": benchmark,
                "cost": cost,
                "gates": {"macro": True, "sector": True, "risk": True},
                "constraints": {"limit": True, "halt": True, "volume": True},
            },
            "metadata": {
                "source": "brain_scheduler",
                "task": "daily_strategy_plan_generation",
            },
        }

    @staticmethod
    def _extract_execution_job_id(payload: Optional[Dict[str, Any]]) -> Optional[str]:
        if not isinstance(payload, dict):
            return None
        candidates = [payload.get("job_id"), payload.get("task_job_id"), payload.get("task_id")]
        job = payload.get("job")
        if isinstance(job, dict):
            candidates.extend([job.get("id"), job.get("job_id")])
        for value in candidates:
            token = str(value or "").strip()
            if token:
                return token
        return None

    async def _trigger_daily_strategy_plan_generation(self, context):
        """触发每日策略计划生成（收盘后生成 T+1 建议单）。"""
        self._mark_task_running("daily_strategy_plan_generation")
        try:
            today = date.today()
            if today.weekday() >= 5:
                self._record_task_execution(
                    "daily_strategy_plan_generation",
                    "skipped",
                    f"Skip strategy plan generation on weekend ({today.isoformat()})",
                )
                return

            execution_adapter = self.app.get("execution_adapter") if self.app else None
            if not execution_adapter:
                self._record_task_execution(
                    "daily_strategy_plan_generation",
                    "skipped",
                    "ExecutionAdapter not available",
                )
                return

            logger.info("Triggering daily strategy plan generation...")
            params = self._build_strategy_plan_job_params(today)
            submit_result = await execution_adapter.submit_ui_job("ui_strategy_report_run", params=params)
            job_id = self._extract_execution_job_id(submit_result)
            if not job_id:
                self._record_task_execution(
                    "daily_strategy_plan_generation",
                    "failed",
                    f"ui_strategy_report_run returned no job_id: {submit_result}",
                )
                return

            timeout_seconds = self._strategy_plan_timeout_seconds()
            job = await execution_adapter.wait_for_job_completion(
                str(job_id),
                timeout=timeout_seconds,
                poll_interval=15,
            )
            status = str(job.get("status") or "")
            if self._is_success_status(status):
                self._record_task_execution(
                    "daily_strategy_plan_generation",
                    "completed",
                    f"Strategy plan generated (job_id={job_id}, status={status})",
                    str(job_id),
                )
            else:
                self._record_task_execution(
                    "daily_strategy_plan_generation",
                    "failed",
                    f"Strategy plan job finished with status={status} (job_id={job_id})",
                    str(job_id),
                )
        except Exception as e:
            logger.error(f"Daily strategy plan generation failed: {e}", exc_info=True)
            self._record_task_execution("daily_strategy_plan_generation", "failed", str(e))

    async def _notify_data_fetch_completed(self, tasks: Optional[List[str]] = None):
        """通知 AnalysisTriggerScheduler 数据抓取任务已完成

        Args:
            tasks: 已完成的任务名称列表；为空时不做通知。
        """
        try:
            if self.app and 'analysis_trigger' in self.app:
                analysis_trigger = self.app['analysis_trigger']

                tasks_to_notify = tasks or []
                if not tasks_to_notify:
                    logger.warning("No completed data tasks to notify")
                    return

                for task_name in tasks_to_notify:
                    await analysis_trigger.mark_task_completed(task_name)
                    logger.info(f"Notified analysis trigger: {task_name} completed")

            else:
                logger.warning("AnalysisTriggerScheduler not available, skipping notification")

        except Exception as e:
            logger.error(f"Failed to notify data fetch completion: {e}", exc_info=True)

    async def _trigger_analysis_cycle(self):
        """触发完整分析周期"""
        self._mark_task_running("full_analysis_cycle")
        try:
            logger.info("Triggering full analysis cycle...")

            if self.coordinator:
                result = await self.coordinator.coordinate_full_analysis_cycle()
                self._record_task_execution("full_analysis_cycle", "completed", f"Analysis cycle: {result.cycle_id}")
            else:
                logger.warning("No coordinator available for analysis cycle")
                self._record_task_execution("full_analysis_cycle", "skipped", "No coordinator available")

        except Exception as e:
            logger.error(f"Analysis cycle failed: {e}")
            self._record_task_execution("full_analysis_cycle", "failed", str(e))

    async def _get_flowhub_adapter(self):
        """获取FlowhubAdapter实例

        Returns:
            FlowhubAdapter: FlowhubAdapter实例，如果不可用则返回None
        """
        try:
            # 创建FlowhubAdapter实例
            if FlowhubAdapter is None:
                logger.warning("FlowhubAdapter import failed")
                return None
            flowhub_adapter = FlowhubAdapter(self.config)

            # 连接到Flowhub服务
            connected = await flowhub_adapter.connect_to_system()

            if connected:
                logger.debug("FlowhubAdapter connected successfully")
                return flowhub_adapter
            else:
                logger.warning("Failed to connect FlowhubAdapter")
                return None

        except Exception as e:
            logger.error(f"Failed to get FlowhubAdapter: {e}")
            return None

    async def _system_health_check(self, context):
        """系统健康检查"""
        self._mark_task_running("system_health_check")
        try:
            logger.debug("Performing system health check...")

            # 简化的健康检查
            health_status = "healthy" if self._is_running else "unhealthy"
            self._record_task_execution("system_health_check", "completed", f"System status: {health_status}")

        except Exception as e:
            logger.error(f"System health check failed: {e}")
            self._record_task_execution("system_health_check", "failed", str(e))

    async def _execute_custom_task(self, task_data: Dict[str, Any]):
        """执行自定义任务"""
        task_name = task_data.get('name', 'custom')
        task_id = task_data.get('task_id')
        self._mark_task_running(task_name, task_id)
        try:
            func = task_data.get('function')
            payload = task_data.get('payload') or {}

            logger.info(f"Executing custom task: {task_name} func={func}")

            if func == 'full_analysis_cycle':
                await self._trigger_analysis_cycle()
            elif func == 'macro_analysis':
                analysis_trigger = self.app.get('analysis_trigger') if self.app else None
                if analysis_trigger:
                    await analysis_trigger._trigger_macro_analysis()
                else:
                    raise AdapterException("IntegrationScheduler", "AnalysisTriggerScheduler not available")
            elif func == 'stock_batch_analysis':
                analysis_trigger = self.app.get('analysis_trigger') if self.app else None
                if analysis_trigger:
                    await analysis_trigger._trigger_stock_batch_analysis()
                else:
                    raise AdapterException("IntegrationScheduler", "AnalysisTriggerScheduler not available")
            elif func == 'daily_data_fetch':
                await self._trigger_daily_data_fetch(None)
            elif func == 'daily_index_fetch':
                await self._trigger_daily_index_fetch(None)
            elif func == 'monthly_sw_industry_full_fetch':
                await self._trigger_monthly_sw_industry_full_fetch(None)
            elif func == 'daily_board_fetch':
                await self._trigger_daily_board_fetch(None)
            elif func == 'daily_strategy_plan_generation':
                await self._trigger_daily_strategy_plan_generation(None)
            else:
                raise AdapterException("IntegrationScheduler", f"Unsupported custom task function: {func}")

            state = self._get_task_state(self._state_key(task_name, task_id))
            if state.get("running"):
                self._record_task_execution(task_name, "completed", "Custom task executed", task_id)

        except Exception as e:
            logger.error(f"Custom task execution failed: {e}")
            self._record_task_execution(task_name, "failed", str(e), task_id)

    def _record_task_execution(self, task_name: str, status: str, message: str, task_id: Optional[str] = None):
        """记录任务执行历史"""
        self._mark_task_finished(task_name, status, task_id)
        execution_record = {
            'task_id': task_id,
            'task_name': task_name,
            'status': status,
            'message': message,
            'timestamp': datetime.utcnow().isoformat()
        }

        self._task_history.append(execution_record)

        # 保持历史记录数量限制
        if len(self._task_history) > 1000:
            self._task_history = self._task_history[-500:]  # 保留最近500条

        logger.info(f"Task execution recorded: {task_name} - {status}")

        # 如果任务成功完成，通知分析触发调度器
        if self._is_success_status(status):
            asyncio.create_task(self._notify_task_completion(task_name))

    def record_task_execution(self, task_name: str, status: str, message: str, task_id: Optional[str] = None):
        """对外暴露的任务执行记录入口（供其他调度器调用）"""
        self._record_task_execution(task_name, status, message, task_id)

    async def _notify_task_completion(self, task_name: str):
        """通知分析触发调度器任务已完成

        Args:
            task_name: 任务名称
        """
        try:
            if self.app and 'analysis_trigger' in self.app:
                analysis_trigger = self.app['analysis_trigger']
                await analysis_trigger.mark_task_completed(task_name)
                logger.info(f"Notified analysis trigger: {task_name} completed")
        except Exception as e:
            logger.warning(f"Failed to notify task completion for {task_name}: {e}")

    async def get_task_history(self, task_id: str, query_params: Dict[str, Any]) -> Dict[str, Any]:
        """获取任务执行历史"""
        limit = int(query_params.get('limit', 10))
        offset = int(query_params.get('offset', 0))

        # 过滤指定任务的历史记录
        task_history = [record for record in self._task_history if record.get('task_id') == task_id or record.get('task_name') == task_id]

        return {
            'task_id': task_id,
            'history': task_history[offset:offset+limit],
            'total': len(task_history),
            'limit': limit,
            'offset': offset
        }

    @staticmethod
    def _is_success_status(status: Optional[str]) -> bool:
        return status in {"completed", "succeeded"}
