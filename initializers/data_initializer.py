"""
Data Initialization Coordinator for brain service

- Waits for dependencies (Flowhub) to be healthy
- Coordinates phased data initialization jobs via FlowhubAdapter
- Persists init state for idempotency and progress reporting
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    from ..config import IntegrationConfig
    from ..adapters.flowhub_adapter import FlowhubAdapter
except Exception:
    from config import IntegrationConfig
    from adapters.flowhub_adapter import FlowhubAdapter

logger = logging.getLogger(__name__)

# Persist init state under application data directory (writable by 'brain' user)
STATE_PATH = "data/brain/init_state.json"


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


@dataclass
class InitPhaseState:
    name: str
    status: str = "pending"  # pending|running|completed|failed|skipped
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    job_ids: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


@dataclass
class InitState:
    version: str = "1.0"
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    last_updated: Optional[str] = None
    phases: Dict[str, InitPhaseState] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "last_updated": self.last_updated,
            "phases": {k: vars(v) for k, v in self.phases.items()},
        }

    @staticmethod
    def from_file(path: str) -> "InitState":
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            phases = {k: InitPhaseState(**v) for k, v in (data.get("phases") or {}).items()}
            return InitState(
                version=data.get("version", "1.0"),
                started_at=data.get("started_at"),
                completed_at=data.get("completed_at"),
                last_updated=data.get("last_updated"),
                phases=phases,
            )
        except FileNotFoundError:
            return InitState()
        except Exception as e:
            logger.warning(f"Failed to read init state: {e}")
            return InitState()

    def save(self, path: str) -> None:
        _ensure_dir(path)
        self.last_updated = _now_iso()
        tmp_path = path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)


class DataInitializationCoordinator:
    def __init__(self, config: IntegrationConfig):
        self.config = config
        self.state_path = STATE_PATH
        self.state = InitState.from_file(self.state_path)
        # define phases
        for name in ("stock_meta", "macro_core", "equities", "macro_rest"):
            if name not in self.state.phases:
                self.state.phases[name] = InitPhaseState(name=name)
        self.state.save(self.state_path)

    async def run(self) -> None:
        """Run initialization in background (non-blocking for app)."""
        logger.info("DataInitializationCoordinator started")
        # idempotency: if already completed recently, skip
        if self.state.completed_at:
            logger.info("Initialization already completed at %s, skipping", self.state.completed_at)
            return
        self.state.started_at = self.state.started_at or _now_iso()
        self.state.save(self.state_path)

        try:
            await self._wait_dependencies()
            # phases in order
            await self._run_phase("stock_meta", self._phase_stock_meta)
            await self._run_phase("macro_core", self._phase_macro_core)
            await self._run_phase("equities", self._phase_equities)
            await self._run_phase("macro_rest", self._phase_macro_rest)
            # mark completed
            self.state.completed_at = _now_iso()
            self.state.save(self.state_path)
            logger.info("Data initialization completed")
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            # keep state for next restart retry

    async def _wait_dependencies(self) -> None:
        deps = getattr(self.config.service, "init_wait_dependencies", ["flowhub"]) or ["flowhub"]
        if "flowhub" not in deps:
            return
        retry_cfg = getattr(self.config.service, "init_retry", {}) or {}
        max_retries = int(retry_cfg.get("max_retries", 10))
        backoff = retry_cfg.get("backoff", [1, 2, 3, 5, 8, 13])
        timeout_total = int(retry_cfg.get("timeout", 300))

        attempt = 0
        start_ts = datetime.utcnow()
        while True:
            attempt += 1
            try:
                adapter = FlowhubAdapter(self.config)
                connected = await adapter.connect_to_system()
                await adapter.disconnect_from_system()
                if connected:
                    logger.info("Flowhub dependency is healthy")
                    return
            except Exception as e:
                logger.warning(f"Flowhub health not ready (attempt {attempt}): {e}")

            if attempt >= max_retries:
                elapsed = (datetime.utcnow() - start_ts).total_seconds()
                if elapsed >= timeout_total:
                    logger.error("Dependency wait timed out, proceeding in degraded mode")
                    return
                # else continue with backoff cycles
                attempt = 0

            delay = backoff[min(len(backoff) - 1, attempt - 1)]
            await asyncio.sleep(delay)

    async def _run_phase(self, name: str, fn) -> None:
        phase = self.state.phases[name]
        if phase.status in ("completed", "skipped"):
            logger.info("Phase %s already %s, skipping", name, phase.status)
            return
        phase.status = "running"
        phase.started_at = phase.started_at or _now_iso()
        self.state.save(self.state_path)
        try:
            jobs = await fn()
            if jobs:
                phase.job_ids.extend(jobs)
            phase.status = "completed"
            phase.completed_at = _now_iso()
            self.state.save(self.state_path)
            logger.info("Phase %s completed with %d jobs", name, len(jobs or []))
        except Exception as e:
            phase.status = "failed"
            phase.errors.append(str(e))
            self.state.save(self.state_path)
            logger.error("Phase %s failed: %s", name, e)
            # do not raise to keep app running

    # ===== Phases =====
    async def _phase_stock_meta(self) -> List[str]:
        # Batch basic data, index info, index daily data and index components snapshot
        jobs: List[str] = []
        adapter = FlowhubAdapter(self.config)
        await adapter.connect_to_system()
        try:
            # 1. 股票基础信息（由 flowhub 根据自身逻辑选择全量/增量）
            req_basic = {"incremental": True}
            resp_basic = await adapter.send_request({
                "method": "POST", "endpoint": "/api/v1/jobs/batch-basic-data", "payload": req_basic
            })
            jobs.append(resp_basic.get("job_id", ""))

            # 2. 指数基础信息初始化
            req_index_info = {"update_mode": "incremental"}
            resp_index_info = await adapter.send_request({
                "method": "POST", "endpoint": "/api/v1/jobs/index-info", "payload": req_index_info
            })
            jobs.append(resp_index_info.get("job_id", ""))

            # 3. 指数日线数据初始化（获取近1年历史数据）
            req_index_daily = {"update_mode": "incremental"}
            resp_index_daily = await adapter.send_request({
                "method": "POST", "endpoint": "/api/v1/jobs/index-daily-data", "payload": req_index_daily
            })
            jobs.append(resp_index_daily.get("job_id", ""))

            # 4. 行业板块数据初始化
            req_industry_board = {"source": "ths", "update_mode": "full_update"}
            resp_industry_board = await adapter.send_request({
                "method": "POST", "endpoint": "/api/v1/jobs/industry-board", "payload": req_industry_board
            })
            jobs.append(resp_industry_board.get("job_id", ""))

            # 5. 概念板块数据初始化
            req_concept_board = {"source": "ths", "update_mode": "full_update"}
            resp_concept_board = await adapter.send_request({
                "method": "POST", "endpoint": "/api/v1/jobs/concept-board", "payload": req_concept_board
            })
            jobs.append(resp_concept_board.get("job_id", ""))

            # 6. 指数成分股快照（由 flowhub 自身在基础数据任务内处理或通过专用策略触发）
            # 此处不再单独触发，避免重复

            return jobs
        finally:
            await adapter.disconnect_from_system()

    async def _phase_macro_core(self) -> List[str]:
        """核心宏观数据初始化阶段

        注意：不再传递 max_history 或任何日期范围参数。
        Flowhub 会根据数据库状态自动确定抓取范围。
        """
        jobs: List[str] = []
        adapter = FlowhubAdapter(self.config)
        await adapter.connect_to_system()
        try:
            concurrency = int(getattr(self.config.service, "init_concurrency", 2) or 2)
            sem = asyncio.Semaphore(concurrency)

            async def _create(dt: str, **kwargs):
                async with sem:
                    # 只传递 incremental=True，不传递 max_history 或日期范围参数
                    resp = await adapter.create_macro_data_job(dt, incremental=True, **kwargs)
                    jobs.append(resp.get("job_id", ""))

            # 核心宏观（月度+日度）：价格指数、货币供应、利率、股指、资金流、商品价格
            # 注意：exchange-rate-data 目前未在 flowhub 路由中实现，先移除避免 405
            tasks = [
                _create("price-index-data"),
                _create("money-supply-data"),
                _create("interest-rate-data"),
                _create("stock-index-data"),
                # 市场资金流数据：明确指定所有类型（MARGIN, NORTHBOUND, TURNOVER）
                _create("market-flow-data", flow_types=["MARGIN", "NORTHBOUND", "TURNOVER"]),
                _create("commodity-price-data"),
            ]
            await asyncio.gather(*tasks)
            return jobs
        finally:
            await adapter.disconnect_from_system()

    async def _phase_equities(self) -> List[str]:
        jobs: List[str] = []
        adapter = FlowhubAdapter(self.config)
        await adapter.connect_to_system()
        try:
            # 股票日K（增量/全量由 flowhub 决定）
            # 这里调用 batch-stock-data 类型任务（由 FlowhubAdapter 提供）
            req = {"incremental": True}
            resp = await adapter.create_batch_stock_data_job(req)
            jobs.append(resp.get("job_id", ""))
            return jobs
        finally:
            await adapter.disconnect_from_system()

    async def _phase_macro_rest(self) -> List[str]:
        """其他宏观数据初始化阶段

        注意：不再传递 max_history 或任何日期范围参数。
        Flowhub 会根据数据库状态自动确定抓取范围。
        """
        jobs: List[str] = []
        adapter = FlowhubAdapter(self.config)
        await adapter.connect_to_system()
        try:
            concurrency = int(getattr(self.config.service, "init_concurrency", 2) or 2)
            sem = asyncio.Semaphore(concurrency)

            async def _create(dt: str, **kwargs):
                async with sem:
                    # 只传递 incremental=True，不传递 max_history 或日期范围参数
                    resp = await adapter.create_macro_data_job(dt, incremental=True, **kwargs)
                    jobs.append(resp.get("job_id", ""))
            # 其他宏观：社融、投资、工业、情绪、库存周期、GDP、创新、人口
            tasks = [
                _create("social-financing-data"),
                _create("investment-data"),
                _create("industrial-data"),
                _create("sentiment-index-data"),
                _create("inventory-cycle-data"),
                _create("gdp-data"),
                _create("innovation-data"),
                _create("demographic-data"),
            ]
            await asyncio.gather(*tasks)
            return jobs
        finally:
            await adapter.disconnect_from_system()

