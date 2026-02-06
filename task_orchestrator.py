"""
统一任务编排器
"""

import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


class TaskOrchestrator:
    SERVICES = ("flowhub", "execution", "macro", "portfolio")

    def __init__(self, app):
        self._app = app
        self._brain_jobs: Dict[str, Dict[str, Any]] = {}
        self._history: Dict[str, List[Dict[str, Any]]] = {}

    async def create_task_job(
        self,
        service: str,
        job_type: str,
        params: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        service_payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        service = (service or "").lower()
        if service not in self.SERVICES:
            raise ValueError(f"Unsupported service: {service}")

        payload = service_payload if isinstance(service_payload, dict) else self._build_create_payload(service, job_type, params or {})
        response = await self._request_service(service, "POST", "/api/v1/jobs", payload=payload)
        service_job_id = self._extract_job_id(response)
        if not service_job_id:
            raise RuntimeError(f"Unable to extract job_id from {service} response")

        task_job_id = str(uuid.uuid4())
        record = {
            "task_job_id": task_job_id,
            "service": service,
            "service_job_id": service_job_id,
            "job_type": job_type,
            "created_at": self._utc_now(),
            "metadata": metadata or {},
        }
        self._brain_jobs[task_job_id] = record

        job_payload = await self._safe_get_service_job(service, service_job_id)
        normalized = self._normalize_job(service, job_payload or {"job_id": service_job_id}, task_job_id=task_job_id)
        normalized["job_type"] = normalized.get("job_type") or job_type
        normalized["metadata"] = {**record["metadata"], **(normalized.get("metadata") or {})}
        self._append_history(task_job_id, "created", normalized)
        return normalized

    async def list_task_jobs(
        self,
        service: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Dict[str, Any]:
        services = [service.lower()] if service else list(self.SERVICES)
        jobs: List[Dict[str, Any]] = []
        errors: List[Dict[str, str]] = []

        for svc in services:
            if svc not in self.SERVICES:
                continue
            try:
                payload = await self._request_service(
                    svc,
                    "GET",
                    "/api/v1/jobs",
                    params={"limit": max(limit, 1), "offset": 0, **({"status": status} if status else {})},
                )
                raw_jobs = self._extract_jobs(payload)
                for raw_job in raw_jobs:
                    service_job_id = self._get_service_job_id(raw_job)
                    if not service_job_id:
                        continue
                    mapped_id = self._find_task_job_id(svc, service_job_id)
                    task_job_id = mapped_id or f"{svc}:{service_job_id}"
                    normalized = self._normalize_job(svc, raw_job, task_job_id=task_job_id)
                    if mapped_id:
                        self._append_history_if_changed(mapped_id, normalized)
                    if status and normalized.get("status") != self._normalize_status(status):
                        continue
                    jobs.append(normalized)
            except Exception as exc:
                errors.append({"service": svc, "error": str(exc)})

        total = len(jobs)
        page = jobs[offset: offset + limit]
        return {
            "jobs": page,
            "total": total,
            "limit": limit,
            "offset": offset,
            "errors": errors,
        }

    async def get_task_job(self, task_job_id: str) -> Dict[str, Any]:
        service, service_job_id, mapped_task_job_id = self._resolve_task_job_id(task_job_id)
        payload = await self._request_service(service, "GET", f"/api/v1/jobs/{service_job_id}")
        normalized = self._normalize_job(service, self._extract_data(payload), task_job_id=mapped_task_job_id or task_job_id)
        if mapped_task_job_id:
            self._append_history_if_changed(mapped_task_job_id, normalized)
        return normalized

    async def cancel_task_job(self, task_job_id: str) -> Dict[str, Any]:
        service, service_job_id, mapped_task_job_id = self._resolve_task_job_id(task_job_id)
        await self._request_service(service, "DELETE", f"/api/v1/jobs/{service_job_id}")
        payload = await self._safe_get_service_job(service, service_job_id)
        normalized = self._normalize_job(
            service,
            payload or {"job_id": service_job_id, "status": "cancelled"},
            task_job_id=mapped_task_job_id or task_job_id,
        )
        if mapped_task_job_id:
            self._append_history(mapped_task_job_id, "cancelled", normalized)
        return normalized

    async def get_task_job_history(self, task_job_id: str) -> Dict[str, Any]:
        try:
            service, service_job_id, mapped_task_job_id = self._resolve_task_job_id(task_job_id)
        except Exception:
            return {"task_job_id": task_job_id, "history": []}

        resolved_id = mapped_task_job_id or task_job_id
        history = list(self._history.get(resolved_id, []))
        if not history:
            payload = await self._safe_get_service_job(service, service_job_id)
            if payload:
                history.append({
                    "event": "snapshot",
                    "timestamp": self._utc_now(),
                    "job": self._normalize_job(service, payload, task_job_id=resolved_id),
                })

        return {"task_job_id": resolved_id, "history": history}

    def _resolve_task_job_id(self, task_job_id: str) -> Tuple[str, str, Optional[str]]:
        if task_job_id in self._brain_jobs:
            record = self._brain_jobs[task_job_id]
            return record["service"], record["service_job_id"], task_job_id

        if ":" in task_job_id:
            service, service_job_id = task_job_id.split(":", 1)
            service = service.lower()
            if service in self.SERVICES and service_job_id:
                return service, service_job_id, None

        raise ValueError(f"Unknown task_job_id: {task_job_id}")

    def _build_create_payload(self, service: str, job_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
        if service == "flowhub":
            flowhub_payload = dict(params)
            flowhub_payload.setdefault("data_type", job_type)
            return flowhub_payload
        return {
            "job_type": job_type,
            "params": params,
        }

    async def _safe_get_service_job(self, service: str, service_job_id: str) -> Optional[Dict[str, Any]]:
        try:
            payload = await self._request_service(service, "GET", f"/api/v1/jobs/{service_job_id}")
            return self._extract_data(payload)
        except Exception:
            return None

    async def _request_service(
        self,
        service: str,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        registry = self._app.get("service_registry")
        service_config = getattr(registry, "_services", {}).get(service) if registry else None
        session = getattr(registry, "_session", None) if registry else None
        if not service_config or session is None:
            raise RuntimeError(f"Service {service} not available")

        url = f"{service_config['url'].rstrip('/')}{path}"
        async with session.request(method, url, json=payload, params=params) as resp:
            text = await resp.text()
            try:
                data = json.loads(text) if text else {}
            except Exception:
                data = {"raw": text}
            if resp.status >= 400:
                raise RuntimeError(f"{service} {method} {path} -> HTTP {resp.status}: {text[:300]}")
            if isinstance(data, dict):
                return data
            return {"data": data}

    @staticmethod
    def _extract_data(payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        data = payload.get("data")
        return data if isinstance(data, dict) else payload

    @classmethod
    def _extract_jobs(cls, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = payload.get("data") if isinstance(payload, dict) else None
        if isinstance(data, dict):
            jobs = data.get("jobs")
            if isinstance(jobs, list):
                return jobs
        if isinstance(payload, dict):
            jobs = payload.get("jobs")
            if isinstance(jobs, list):
                return jobs
        if isinstance(payload, list):
            return payload
        return []

    def _extract_job_id(self, payload: Dict[str, Any]) -> Optional[str]:
        candidates: List[Any] = []
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, dict):
                candidates.extend([data.get("job_id"), data.get("task_id"), data.get("id")])
                job_obj = data.get("job")
                if isinstance(job_obj, dict):
                    candidates.extend([job_obj.get("job_id"), job_obj.get("task_id"), job_obj.get("id")])
            candidates.extend([payload.get("job_id"), payload.get("task_id"), payload.get("id")])
        for value in candidates:
            if isinstance(value, str) and value:
                return value
        return None

    @staticmethod
    def _get_service_job_id(job: Dict[str, Any]) -> Optional[str]:
        for key in ("job_id", "task_id", "id"):
            value = job.get(key)
            if isinstance(value, str) and value:
                return value
        return None

    def _find_task_job_id(self, service: str, service_job_id: str) -> Optional[str]:
        for task_job_id, record in self._brain_jobs.items():
            if record.get("service") == service and record.get("service_job_id") == service_job_id:
                return task_job_id
        return None

    def _normalize_job(self, service: str, job: Dict[str, Any], task_job_id: str) -> Dict[str, Any]:
        job = job if isinstance(job, dict) else {}
        service_job_id = self._get_service_job_id(job) or task_job_id
        status = self._normalize_status(job.get("status") or job.get("state") or job.get("job_status"))
        progress = self._normalize_progress(job.get("progress"), status)
        params = job.get("params") if isinstance(job.get("params"), dict) else {}
        metadata = job.get("metadata") if isinstance(job.get("metadata"), dict) else {}
        message = metadata.get("message") or job.get("message")
        job_type = (
            job.get("job_type")
            or job.get("type")
            or job.get("task_type")
            or job.get("data_type")
            or params.get("job_type")
            or params.get("data_type")
            or "unknown"
        )
        created_at = job.get("created_at")
        started_at = job.get("started_at") or job.get("start_time")
        updated_at = job.get("updated_at") or job.get("last_update")
        completed_at = job.get("completed_at") or job.get("end_time")
        if updated_at is None:
            updated_at = completed_at or started_at or created_at

        return {
            "id": task_job_id,
            "service": service,
            "service_job_id": service_job_id,
            "job_type": job_type,
            "status": status,
            "progress": progress,
            "cancellable": status in {"queued", "running"},
            "message": message,
            "error": job.get("error"),
            "result": job.get("result"),
            "created_at": created_at,
            "started_at": started_at,
            "updated_at": updated_at,
            "completed_at": completed_at,
            "metadata": metadata,
        }

    @staticmethod
    def _normalize_status(status: Any) -> str:
        if not status:
            return "queued"
        value = str(status).strip().lower()
        if value in {"queued", "pending", "submitted", "accepted", "created", "idle"}:
            return "queued"
        if value in {"running", "in_progress", "processing", "partially_filled"}:
            return "running"
        if value in {"succeeded", "success", "completed", "done"}:
            return "succeeded"
        if value in {"failed", "error", "timeout"}:
            return "failed"
        if value in {"cancelled", "canceled"}:
            return "cancelled"
        return "failed"

    @staticmethod
    def _normalize_progress(progress: Any, status: str) -> int:
        if progress is not None:
            try:
                value = int(float(progress))
                return max(0, min(100, value))
            except Exception:
                pass
        if status == "succeeded":
            return 100
        if status in {"failed", "cancelled"}:
            return 0
        return 0

    def _append_history(self, task_job_id: str, event: str, normalized_job: Dict[str, Any]) -> None:
        history = self._history.setdefault(task_job_id, [])
        history.append(
            {
                "event": event,
                "timestamp": self._utc_now(),
                "job": dict(normalized_job),
            }
        )

    def _append_history_if_changed(self, task_job_id: str, normalized_job: Dict[str, Any]) -> None:
        history = self._history.setdefault(task_job_id, [])
        if not history:
            self._append_history(task_job_id, "snapshot", normalized_job)
            return
        last_job = history[-1].get("job", {})
        if (
            last_job.get("status") != normalized_job.get("status")
            or last_job.get("progress") != normalized_job.get("progress")
            or last_job.get("updated_at") != normalized_job.get("updated_at")
        ):
            self._append_history(task_job_id, "snapshot", normalized_job)

    @staticmethod
    def _utc_now() -> str:
        return datetime.utcnow().isoformat() + "Z"
