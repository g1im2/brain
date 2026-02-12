"""
Unified UI BFF proxy handler.
"""

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import re
import sys
import uuid
from typing import Any, Callable, Dict, Optional, Pattern

from aiohttp import web

from handlers.base import BaseHandler
from task_orchestrator import UpstreamServiceError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ECONDB_PATH = PROJECT_ROOT / "external" / "econdb"
if ECONDB_PATH.exists() and str(ECONDB_PATH) not in sys.path:
    sys.path.insert(0, str(ECONDB_PATH))

try:
    from econdb import (  # type: ignore
        UISystemDataAPI,
        SystemAuditDTO,
        SystemSettingDTO,
        SystemUserDTO,
        create_database_manager,
    )
except Exception:  # pragma: no cover - import fallback for old submodule commits
    UISystemDataAPI = None
    SystemAuditDTO = None
    SystemSettingDTO = None
    SystemUserDTO = None
    create_database_manager = None


HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
}

CORS_HEADERS = {
    "access-control-allow-origin",
    "access-control-allow-credentials",
    "access-control-allow-methods",
    "access-control-allow-headers",
    "access-control-expose-headers",
    "access-control-max-age",
}


@dataclass(frozen=True)
class RouteSpec:
    method: str
    pattern: Pattern[str]
    service: str
    is_mutation: bool = False
    job_type: Optional[str] = None
    params_builder: Optional[Callable[[Dict[str, Any], Dict[str, str]], Dict[str, Any]]] = None


class UIBffHandler(BaseHandler):
    """Brain-side frontend BFF for /api/v1/ui/*."""

    ROUTES = (
        RouteSpec("GET", re.compile(r"^/api/v1/ui/market-snapshot/list$"), "macro"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/market-snapshot/alerts$"), "macro"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/market-snapshot/latest$"), "macro"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/market-snapshot/(?P<snapshot_id>[^/]+)$"), "macro"),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/market-snapshot/(?P<snapshot_id>[^/]+)/evidence/(?P<ev_key>[^/]+)$"),
            "macro",
        ),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/market-snapshot/(?P<snapshot_id>[^/]+)/compare$"),
            "macro",
        ),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/market-snapshot/(?P<snapshot_id>[^/]+)/export$"),
            "macro",
        ),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/macro-cycle/inbox$"), "macro"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/macro-cycle/history$"), "macro"),
        RouteSpec("POST", re.compile(r"^/api/v1/ui/macro-cycle/freeze$"), "macro", True, "ui_macro_cycle_freeze"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/macro-cycle/(?P<snapshot_id>[^/]+)$"), "macro"),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/macro-cycle/(?P<snapshot_id>[^/]+)/evidence/(?P<key>[^/]+)$"),
            "macro",
        ),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/macro-cycle/calendar$"), "macro"),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/macro-cycle/(?P<snapshot_id>[^/]+)/mark-seen$"),
            "macro",
            True,
            "ui_macro_cycle_freeze",
            lambda payload, params: {**payload, "snapshot_id": params["snapshot_id"]},
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/macro-cycle/(?P<snapshot_id>[^/]+)/apply/portfolio$"),
            "macro",
            True,
            "ui_rotation_policy_apply",
            lambda payload, params: {**payload, "snapshot_id": params["snapshot_id"]},
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/macro-cycle/(?P<snapshot_id>[^/]+)/apply/snapshot$"),
            "macro",
            True,
            "ui_rotation_policy_apply",
            lambda payload, params: {**payload, "snapshot_id": params["snapshot_id"]},
        ),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/structure-rotation/overview$"), "macro"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/structure-rotation/policy/current$"), "macro"),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/structure-rotation/policy/freeze$"),
            "macro",
            True,
            "ui_rotation_policy_freeze",
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/structure-rotation/policy/apply$"),
            "macro",
            True,
            "ui_rotation_policy_apply",
        ),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/structure-rotation/policy/diff$"), "macro"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/candidates/clusters$"), "execution"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/candidates/events$"), "execution"),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/candidates/history/query$"),
            "execution",
            True,
            "ui_candidates_history_query",
        ),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/candidates/history/(?P<query_id>[^/]+)$"), "execution"),
        RouteSpec("POST", re.compile(r"^/api/v1/ui/candidates/promote$"), "execution", True, "ui_candidates_promote"),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/candidates/auto-promote$"),
            "execution",
            True,
            "ui_candidates_promote",
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/candidates/merge$"),
            "execution",
            True,
            "ui_candidates_history_query",
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/candidates/(?P<id>[^/]+)/ignore$"),
            "execution",
            True,
            "ui_candidates_promote",
            lambda payload, params: {**payload, "event_id": params["id"], "ignore_only": True},
        ),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/candidates/export$"), "execution"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/research/subjects$"), "execution"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/research/subjects/(?P<subject_id>[^/]+)$"), "execution"),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/research/subjects/(?P<subject_id>[^/]+)/decision$"),
            "execution",
            True,
            "ui_research_decision",
            lambda payload, params: {**payload, "subject_id": params["subject_id"]},
        ),
        RouteSpec("POST", re.compile(r"^/api/v1/ui/research/freeze$"), "execution", True, "ui_research_freeze"),
        RouteSpec("POST", re.compile(r"^/api/v1/ui/research/compare$"), "execution", True, "ui_research_compare"),
        RouteSpec(
            "PATCH",
            re.compile(r"^/api/v1/ui/research/subjects/(?P<subject_id>[^/]+)/archive$"),
            "execution",
            True,
            "ui_research_freeze",
            lambda payload, params: {**payload, "subject_id": params["subject_id"], "archive": True},
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/research/subjects/(?P<subject_id>[^/]+)/unfreeze$"),
            "execution",
            True,
            "ui_research_freeze",
            lambda payload, params: {**payload, "subject_id": params["subject_id"], "unfreeze": True},
        ),
        RouteSpec("POST", re.compile(r"^/api/v1/ui/research/replace-helper$"), "execution", True, "ui_research_compare"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/research/export$"), "execution"),
        RouteSpec("POST", re.compile(r"^/api/v1/ui/strategy/config/apply$"), "execution", True, "ui_strategy_report_run"),
        RouteSpec("POST", re.compile(r"^/api/v1/ui/strategy/presets$"), "execution", True, "ui_strategy_report_compare"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/strategy/presets$"), "execution"),
        RouteSpec("POST", re.compile(r"^/api/v1/ui/strategy/reports/run$"), "execution", True, "ui_strategy_report_run"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/strategy/reports$"), "execution"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/strategy/reports/(?P<report_id>[^/]+)$"), "execution"),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/strategy/reports/compare$"),
            "execution",
            True,
            "ui_strategy_report_compare",
        ),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/strategy/reports/(?P<report_id>[^/]+)/lineage/export$"),
            "execution",
        ),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/strategy/reports/(?P<report_id>[^/]+)/analysis$"),
            "execution",
        ),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/strategy/reports/(?P<report_id>[^/]+)/trades/export$"),
            "execution",
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/execution/sim/orders$"),
            "portfolio",
            True,
            "ui_sim_order_create",
        ),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/execution/sim/orders$"), "portfolio"),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/execution/sim/orders/(?P<order_id>[^/]+)/cancel$"),
            "portfolio",
            True,
            "ui_sim_order_cancel",
            lambda payload, params: {**payload, "order_id": params["order_id"]},
        ),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/execution/sim/positions$"), "portfolio"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/execution/sim/pnl$"), "portfolio"),
    )

    SHELL_CONTEXT_PAGES = {
        "overview": {"phase": "市场快照", "page_title": "市场快照", "service": "macro"},
        "macro": {"phase": "宏观与周期", "page_title": "宏观与周期", "service": "macro"},
        "structure": {"phase": "结构与轮动", "page_title": "结构与轮动", "service": "macro"},
        "watchlist": {"phase": "候选池", "page_title": "候选池", "service": "execution"},
        "stock": {"phase": "标的研究", "page_title": "标的研究", "service": "execution"},
        "strategy": {"phase": "策略与验证", "page_title": "策略与验证", "service": "execution"},
        "jobs": {"phase": "系统任务", "page_title": "任务中心", "service": "brain"},
        "data": {"phase": "系统数据", "page_title": "数据运维", "service": "flowhub"},
        "settings": {"phase": "系统设置", "page_title": "设置中心", "service": "brain"},
        "users": {"phase": "系统用户", "page_title": "用户与权限", "service": "brain"},
    }

    def __init__(self):
        super().__init__()
        self._system_api = None

    @staticmethod
    def _match_route(method: str, path: str) -> tuple[Optional[RouteSpec], Dict[str, str]]:
        effective_method = "GET" if method.upper() == "HEAD" else method.upper()
        for route in UIBffHandler.ROUTES:
            if route.method != effective_method:
                continue
            matched = route.pattern.match(path)
            if matched:
                return route, matched.groupdict()
        return None, {}

    def _build_job_params(
        self,
        route: RouteSpec,
        request_payload: Dict[str, Any],
        path_params: Dict[str, str],
    ) -> Dict[str, Any]:
        payload = dict(request_payload or {})
        if route.params_builder:
            return route.params_builder(payload, path_params)
        return payload

    def _get_system_api(self):
        if UISystemDataAPI is None or create_database_manager is None:
            raise RuntimeError("UISystemDataAPI unavailable; update external/econdb submodule baseline")
        if self._system_api is None:
            db_manager = create_database_manager()
            self._system_api = UISystemDataAPI(db_manager)
            self._system_api.ensure_system_schema()
            self._system_api.seed_defaults()
        return self._system_api

    @staticmethod
    def _unwrap_response_data(payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {"raw": payload}
        if "data" in payload and isinstance(payload["data"], dict):
            return payload["data"]
        return payload

    async def _fetch_upstream_json(
        self,
        request: web.Request,
        service_name: str,
        path: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        registry = self.get_app_component(request, "service_registry")
        service = getattr(registry, "_services", {}).get(service_name)
        session = getattr(registry, "_session", None)
        if not service or session is None:
            raise RuntimeError(f"Service not available: {service_name}")

        target_url = f"{service['url'].rstrip('/')}{path}"
        async with session.request(method.upper(), target_url, params=params, json=payload) as resp:
            raw = await resp.text()
            body: Dict[str, Any]
            try:
                body = json.loads(raw) if raw else {}
            except Exception:
                body = {"raw": raw}
            if resp.status >= 400:
                raise UpstreamServiceError(
                    service=service_name,
                    method=method.upper(),
                    path=path,
                    status=resp.status,
                    error={"message": body.get("error") or body.get("message") or "Upstream error", "raw": body},
                )
            if not isinstance(body, dict):
                return {"raw": body}
            return body

    async def _handle_shell_context(self, request: web.Request) -> web.Response:
        page = (request.query.get("page") or "overview").strip().lower()
        context = dict(self.SHELL_CONTEXT_PAGES.get(page, self.SHELL_CONTEXT_PAGES["overview"]))
        orchestrator = self.get_app_component(request, "task_orchestrator")
        task_data = await orchestrator.list_task_jobs(limit=20, offset=0)
        jobs = task_data.get("jobs", [])
        running_count = sum(1 for item in jobs if item.get("status") in ("queued", "running"))
        failed_count = sum(1 for item in jobs if item.get("status") == "failed")
        cancellable_count = sum(1 for item in jobs if bool(item.get("cancellable")))

        user_info: Dict[str, Any] = {
            "id": "user_admin",
            "username": "admin",
            "display_name": "Admin",
            "roles": [{"id": "role_admin", "name": "admin"}],
        }
        notifications = []
        try:
            system_api = self._get_system_api()
            user_info = system_api.get_user("user_admin") or user_info
            notifications = system_api.list_notifications(limit=5)
        except Exception as exc:
            self.logger.warning(f"system api unavailable for shell context: {exc}")

        payload = {
            "page": page,
            "phase": context["phase"],
            "page_title": context["page_title"],
            "service": context["service"],
            "topbar_chips": [
                {"key": "running_jobs", "label": "运行中任务", "value": running_count},
                {"key": "failed_jobs", "label": "失败任务", "value": failed_count},
                {"key": "cancellable_jobs", "label": "可取消", "value": cancellable_count},
            ],
            "toolbar_actions": [
                {"id": "refresh", "label": "刷新", "enabled": True},
                {"id": "search", "label": "搜索", "enabled": True},
                {"id": "open_tasks", "label": "任务中心", "enabled": True},
            ],
            "sidebar_badges": {
                "jobs": running_count,
                "alerts": failed_count,
                "notifications": len(notifications),
            },
            "current_user": user_info,
            "notifications": notifications,
        }
        return self.success_response(payload)

    @staticmethod
    def _contains_query(q: str, *values: Any) -> bool:
        query = q.lower()
        for value in values:
            if value is None:
                continue
            if query in str(value).lower():
                return True
        return False

    async def _handle_search(self, request: web.Request) -> web.Response:
        q = (request.query.get("q") or "").strip()
        scope = (request.query.get("scope") or "all").strip().lower()
        limit = int(request.query.get("limit", 20))
        if not q:
            return self.success_response({"items": [], "total": 0, "scope": scope, "query": q})

        items = []
        if scope in ("all", "research", "subjects"):
            try:
                payload = await self._fetch_upstream_json(request, "execution", "/api/v1/ui/research/subjects", params={"limit": 200, "offset": 0})
                for row in self._unwrap_response_data(payload).get("items", []):
                    if self._contains_query(q, row.get("id"), row.get("symbol"), row.get("title"), row.get("status")):
                        items.append({"type": "research_subject", "id": row.get("id"), "title": row.get("title"), "service": "execution", "payload": row})
            except Exception as exc:
                self.logger.warning(f"search subjects failed: {exc}")

        if scope in ("all", "candidates", "events"):
            try:
                payload = await self._fetch_upstream_json(request, "execution", "/api/v1/ui/candidates/events", params={"limit": 200, "offset": 0})
                for row in self._unwrap_response_data(payload).get("items", []):
                    if self._contains_query(q, row.get("id"), row.get("symbol"), row.get("signal_type"), row.get("status")):
                        items.append({"type": "candidate_event", "id": row.get("id"), "title": row.get("symbol"), "service": "execution", "payload": row})
            except Exception as exc:
                self.logger.warning(f"search events failed: {exc}")

        if scope in ("all", "macro"):
            try:
                payload = await self._fetch_upstream_json(request, "macro", "/api/v1/ui/macro-cycle/history", params={"limit": 200, "offset": 0})
                for row in self._unwrap_response_data(payload).get("items", []):
                    if self._contains_query(q, row.get("id"), row.get("cycle_phase"), row.get("status")):
                        items.append({"type": "macro_cycle", "id": row.get("id"), "title": row.get("cycle_phase"), "service": "macro", "payload": row})
            except Exception as exc:
                self.logger.warning(f"search macro history failed: {exc}")

        items = items[: max(limit, 1)]
        return self.success_response({"items": items, "total": len(items), "scope": scope, "query": q})

    async def _handle_system_jobs_overview(self, request: web.Request) -> web.Response:
        orchestrator = self.get_app_component(request, "task_orchestrator")
        service = request.query.get("service")
        status = request.query.get("status")
        limit = int(request.query.get("limit", 50))
        offset = int(request.query.get("offset", 0))
        payload = await orchestrator.list_task_jobs(service=service, status=status, limit=limit, offset=offset)
        jobs = payload.get("jobs", [])
        summary = {
            "queued": sum(1 for item in jobs if item.get("status") == "queued"),
            "running": sum(1 for item in jobs if item.get("status") == "running"),
            "succeeded": sum(1 for item in jobs if item.get("status") == "succeeded"),
            "failed": sum(1 for item in jobs if item.get("status") == "failed"),
            "cancelled": sum(1 for item in jobs if item.get("status") == "cancelled"),
        }
        return self.success_response({**payload, "summary": summary})

    async def _handle_system_cancel(self, request: web.Request, task_job_id: Optional[str] = None) -> web.Response:
        task_job_id = task_job_id or request.match_info.get("task_job_id")
        if not task_job_id:
            return self.error_response("Missing task_job_id", 400)
        orchestrator = self.get_app_component(request, "task_orchestrator")
        payload = await orchestrator.cancel_task_job(task_job_id)
        return self.success_response(payload, "Task cancellation requested")

    async def _handle_system_history(self, request: web.Request, task_job_id: Optional[str] = None) -> web.Response:
        task_job_id = task_job_id or request.match_info.get("task_job_id")
        if not task_job_id:
            return self.error_response("Missing task_job_id", 400)
        orchestrator = self.get_app_component(request, "task_orchestrator")
        payload = await orchestrator.get_task_job_history(task_job_id)
        return self.success_response(payload)

    async def _handle_system_data_overview(self, request: web.Request) -> web.Response:
        orchestrator = self.get_app_component(request, "task_orchestrator")
        flowhub_jobs = await orchestrator.list_task_jobs(service="flowhub", limit=100, offset=0)
        jobs = flowhub_jobs.get("jobs", [])
        notifications = []
        try:
            notifications = self._get_system_api().list_notifications(limit=20)
        except Exception as exc:
            self.logger.warning(f"list notifications for data overview failed: {exc}")
        payload = {
            "flowhub_jobs": jobs,
            "flowhub_summary": {
                "total": len(jobs),
                "running": sum(1 for item in jobs if item.get("status") in ("queued", "running")),
                "failed": sum(1 for item in jobs if item.get("status") == "failed"),
                "succeeded": sum(1 for item in jobs if item.get("status") == "succeeded"),
            },
            "notifications": notifications,
        }
        return self.success_response(payload)

    async def _create_flowhub_job(self, request: web.Request, job_type: str, payload: Optional[Dict[str, Any]] = None) -> web.Response:
        orchestrator = self.get_app_component(request, "task_orchestrator")
        created = await orchestrator.create_task_job(
            service="flowhub",
            job_type=job_type,
            params=payload or {},
            metadata={
                "ui_path": request.path,
                "ui_method": request.method,
                "query": dict(request.query),
                "created_at": datetime.utcnow().isoformat() + "Z",
            },
        )
        task_job_id = created["id"]
        return web.json_response(
            {
                "success": True,
                "status": "accepted",
                "message": "Task accepted",
                "data": {
                    "task_job_id": task_job_id,
                    "status_url": f"/api/v1/task-jobs/{task_job_id}",
                    "cancel_url": f"/api/v1/task-jobs/{task_job_id}/cancel",
                    "history_url": f"/api/v1/task-jobs/{task_job_id}/history",
                    "service": "flowhub",
                    "job_type": job_type,
                },
                "timestamp": datetime.utcnow().isoformat() + "Z",
            },
            status=202,
        )

    async def _handle_system_data_sync(self, request: web.Request) -> web.Response:
        payload = {}
        if request.can_read_body:
            payload = await self.get_request_json(request)
            if payload is None:
                payload = {}
            if not isinstance(payload, dict):
                return self.error_response("Request body must be a JSON object", 400)
        return await self._create_flowhub_job(request, "backfill_data_type_history", payload)

    async def _handle_system_data_backfill(self, request: web.Request, action: str) -> web.Response:
        payload = {}
        if request.can_read_body:
            payload = await self.get_request_json(request)
            if payload is None:
                payload = {}
            if not isinstance(payload, dict):
                return self.error_response("Request body must be a JSON object", 400)
        job_type_map = {
            "full-history": "backfill_full_history",
            "resume": "backfill_resume_run",
            "retry-failed": "backfill_retry_failed_shards",
        }
        if action not in job_type_map:
            return self.error_response("Unsupported backfill action", 404)
        return await self._create_flowhub_job(request, job_type_map[action], payload)

    async def _handle_system_settings_list(self, request: web.Request) -> web.Response:
        api = self._get_system_api()
        payload = {
            "settings": api.list_settings(),
            "presets": api.list_presets(),
        }
        return self.success_response(payload)

    async def _handle_system_settings_upsert(self, request: web.Request, key: Optional[str] = None) -> web.Response:
        key = key or request.match_info.get("key")
        if not key:
            return self.error_response("Missing settings key", 400)
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        value = body.get("value") if body.get("value") is not None else {}
        metadata = body.get("metadata") if isinstance(body.get("metadata"), dict) else {}
        updated_by = body.get("updated_by") or "user_admin"

        api = self._get_system_api()
        setting_payload = api.upsert_setting(
            SystemSettingDTO(
                key=key,
                value=value if isinstance(value, dict) else {"value": value},
                metadata=metadata,
                updated_by=str(updated_by),
            )
        )
        api.append_audit_log(
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=str(updated_by),
                action="setting.upsert",
                target_type="setting",
                target_id=key,
                payload={"value": value, "metadata": metadata},
                created_at=datetime.utcnow().isoformat(),
            )
        )
        return self.success_response(setting_payload, "Setting updated")

    async def _handle_system_users_list(self, request: web.Request) -> web.Response:
        limit = int(request.query.get("limit", 100))
        offset = int(request.query.get("offset", 0))
        status = request.query.get("status")
        api = self._get_system_api()
        payload = {
            "items": api.list_users(status=status, limit=limit, offset=offset),
            "roles": api.list_roles(),
            "permissions": api.list_permissions(),
            "limit": limit,
            "offset": offset,
        }
        return self.success_response(payload)

    async def _handle_system_users_create(self, request: web.Request) -> web.Response:
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        username = (body.get("username") or "").strip()
        display_name = (body.get("display_name") or "").strip() or username
        if not username:
            return self.error_response("Missing required field: username", 400)
        actor_id = body.get("actor_id") or "user_admin"
        user_id = body.get("id") or str(uuid.uuid4())
        api = self._get_system_api()
        user = api.upsert_user(
            SystemUserDTO(
                id=user_id,
                username=username,
                display_name=display_name,
                email=body.get("email"),
                status=body.get("status") or "active",
                metadata=body.get("metadata") if isinstance(body.get("metadata"), dict) else {},
            )
        )
        role_ids = body.get("role_ids") if isinstance(body.get("role_ids"), list) else []
        if role_ids:
            api.replace_user_roles(user_id, [str(role_id) for role_id in role_ids], actor_id=str(actor_id))
            user = api.get_user(user_id) or user
        api.append_audit_log(
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=str(actor_id),
                action="user.create",
                target_type="user",
                target_id=user_id,
                payload={"username": username, "role_ids": role_ids},
                created_at=datetime.utcnow().isoformat(),
            )
        )
        return self.success_response(user, "User created")

    async def _handle_system_users_update(self, request: web.Request, user_id: Optional[str] = None) -> web.Response:
        user_id = user_id or request.match_info.get("user_id")
        if not user_id:
            return self.error_response("Missing user_id", 400)
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        api = self._get_system_api()
        existing = api.get_user(user_id)
        if not existing:
            return self.error_response("User not found", 404)
        actor_id = body.get("actor_id") or "user_admin"
        updated = api.upsert_user(
            SystemUserDTO(
                id=user_id,
                username=(body.get("username") or existing.get("username")),
                display_name=(body.get("display_name") or existing.get("display_name")),
                email=body.get("email", existing.get("email")),
                status=body.get("status") or existing.get("status") or "active",
                metadata=body.get("metadata") if isinstance(body.get("metadata"), dict) else existing.get("metadata", {}),
            )
        )
        if isinstance(body.get("role_ids"), list):
            api.replace_user_roles(user_id, [str(role_id) for role_id in body["role_ids"]], actor_id=str(actor_id))
            updated = api.get_user(user_id) or updated
        api.append_audit_log(
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=str(actor_id),
                action="user.update",
                target_type="user",
                target_id=user_id,
                payload={"changes": body},
                created_at=datetime.utcnow().isoformat(),
            )
        )
        return self.success_response(updated, "User updated")

    async def _handle_system_audit_list(self, request: web.Request) -> web.Response:
        api = self._get_system_api()
        limit = int(request.query.get("limit", 100))
        offset = int(request.query.get("offset", 0))
        actor_id = request.query.get("actor_id")
        action = request.query.get("action")
        logs = api.list_audit_logs(limit=limit, offset=offset, actor_id=actor_id, action=action)
        return self.success_response({"items": logs, "total": len(logs), "limit": limit, "offset": offset})

    async def _handle_internal_route(self, request: web.Request) -> Optional[web.Response]:
        path = request.path
        method = request.method.upper()

        if method == "GET" and path == "/api/v1/ui/shell/context":
            return await self._handle_shell_context(request)
        if method == "GET" and path == "/api/v1/ui/search":
            return await self._handle_search(request)

        if method == "GET" and path == "/api/v1/ui/system/jobs/overview":
            return await self._handle_system_jobs_overview(request)
        if method == "POST":
            matched = re.match(r"^/api/v1/ui/system/jobs/(?P<task_job_id>[^/]+)/cancel$", path)
            if matched:
                return await self._handle_system_cancel(request, matched.group("task_job_id"))
        if method == "GET":
            matched = re.match(r"^/api/v1/ui/system/jobs/(?P<task_job_id>[^/]+)/history$", path)
            if matched:
                return await self._handle_system_history(request, matched.group("task_job_id"))

        if method == "GET" and path == "/api/v1/ui/system/data/overview":
            return await self._handle_system_data_overview(request)
        if method == "POST" and path == "/api/v1/ui/system/data/sync":
            return await self._handle_system_data_sync(request)
        if method == "POST":
            matched = re.match(r"^/api/v1/ui/system/data/backfill/(?P<action>[^/]+)$", path)
            if matched:
                return await self._handle_system_data_backfill(request, matched.group("action"))

        if method == "GET" and path == "/api/v1/ui/system/settings":
            return await self._handle_system_settings_list(request)
        if method == "PUT":
            matched = re.match(r"^/api/v1/ui/system/settings/(?P<key>[^/]+)$", path)
            if matched:
                return await self._handle_system_settings_upsert(request, matched.group("key"))

        if method == "GET" and path == "/api/v1/ui/system/users":
            return await self._handle_system_users_list(request)
        if method == "POST" and path == "/api/v1/ui/system/users":
            return await self._handle_system_users_create(request)
        if method == "PUT":
            matched = re.match(r"^/api/v1/ui/system/users/(?P<user_id>[^/]+)$", path)
            if matched:
                return await self._handle_system_users_update(request, matched.group("user_id"))

        if method == "GET" and path == "/api/v1/ui/system/audit":
            return await self._handle_system_audit_list(request)

        return None

    async def _proxy_upstream(self, request: web.Request, service_name: str) -> web.Response:
        registry = self.get_app_component(request, "service_registry")
        service = getattr(registry, "_services", {}).get(service_name)
        session = getattr(registry, "_session", None)
        if not service or session is None:
            return self.error_response(f"Service not available: {service_name}", 503)

        target_url = f"{service['url'].rstrip('/')}{request.path_qs}"
        body = await request.read() if request.can_read_body else None
        headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP_HEADERS}
        try:
            async with session.request(request.method, target_url, headers=headers, data=body) as resp:
                response_body = await resp.read()
                response_headers = {
                    k: v
                    for k, v in resp.headers.items()
                    if k.lower() not in HOP_BY_HOP_HEADERS and k.lower() not in CORS_HEADERS
                }
                return web.Response(status=resp.status, headers=response_headers, body=response_body)
        except Exception as exc:
            self.logger.error(f"UI BFF proxy failed: {exc}")
            return self.error_response("UI upstream request failed", 502)

    async def proxy(self, request: web.Request) -> web.Response:
        internal_response = await self._handle_internal_route(request)
        if internal_response is not None:
            return internal_response

        route, path_params = self._match_route(request.method, request.path)
        if not route:
            return self.error_response("Unsupported /api/v1/ui path", 404)

        if not route.is_mutation:
            return await self._proxy_upstream(request, route.service)

        request_payload: Dict[str, Any] = {}
        if request.can_read_body:
            try:
                request_payload = await self.get_request_json(request)
            except web.HTTPBadRequest as exc:
                return self.error_response(exc.text or "Invalid JSON format", 400)
            if not isinstance(request_payload, dict):
                return self.error_response("Request body must be a JSON object", 400)

        try:
            orchestrator = self.get_app_component(request, "task_orchestrator")
            params = self._build_job_params(route, request_payload, path_params)
            metadata = {
                "ui_path": request.path,
                "ui_method": request.method,
                "query": dict(request.query),
                "created_at": datetime.utcnow().isoformat() + "Z",
            }
            created = await orchestrator.create_task_job(
                service=route.service,
                job_type=route.job_type or "unknown",
                params=params,
                metadata=metadata,
            )
            task_job_id = created["id"]
            payload = {
                "task_job_id": task_job_id,
                "status_url": f"/api/v1/task-jobs/{task_job_id}",
                "cancel_url": f"/api/v1/task-jobs/{task_job_id}/cancel",
                "history_url": f"/api/v1/task-jobs/{task_job_id}/history",
                "service": route.service,
                "job_type": route.job_type,
            }
            return web.json_response(
                {
                    "success": True,
                    "status": "accepted",
                    "message": "Task accepted",
                    "data": payload,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                },
                status=202,
            )
        except ValueError as exc:
            return self.error_response(str(exc), 400)
        except UpstreamServiceError as exc:
            return web.json_response(
                {
                    "success": False,
                    "error": exc.error.get("message"),
                    "error_code": "UPSTREAM_REQUEST_FAILED",
                    "upstream_status": exc.status,
                    "upstream_service": exc.service,
                    "details": exc.error.get("raw"),
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                },
                status=exc.status if 400 <= exc.status < 500 else 502,
            )
        except Exception as exc:
            self.logger.error(f"UI mutation orchestration failed: {exc}")
            return self.error_response("UI mutation orchestration failed", 502)
