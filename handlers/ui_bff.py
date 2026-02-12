"""
Unified UI BFF proxy handler.
"""

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any, Callable, Dict, Optional, Pattern

from aiohttp import web

from handlers.base import BaseHandler
from task_orchestrator import UpstreamServiceError


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
