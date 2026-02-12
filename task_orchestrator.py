"""
统一任务编排器
"""

import hashlib
import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


class UpstreamServiceError(RuntimeError):
    """Raised when upstream service returns non-success HTTP status."""

    def __init__(self, service: str, method: str, path: str, status: int, error: Dict[str, Any]):
        self.service = service
        self.method = method
        self.path = path
        self.status = status
        self.error = error
        super().__init__(f"{service} {method} {path} -> HTTP {status}")


class TaskOrchestrator:
    SERVICES = ("flowhub", "execution", "macro", "portfolio")
    SERVICE_JOB_TYPES = {
        "macro": {
            "ui_macro_cycle_freeze",
            "ui_macro_cycle_mark_seen",
            "ui_macro_cycle_apply_portfolio",
            "ui_macro_cycle_apply_snapshot",
            "ui_rotation_policy_freeze",
            "ui_rotation_policy_apply",
        },
        "execution": {
            "ui_candidates_history_query",
            "ui_candidates_promote",
            "ui_candidates_auto_promote",
            "ui_candidates_merge",
            "ui_candidates_ignore",
            "ui_research_decision",
            "ui_research_freeze",
            "ui_research_compare",
            "ui_research_archive",
            "ui_research_unfreeze",
            "ui_research_replace_helper",
            "ui_strategy_report_run",
            "ui_strategy_report_compare",
            "ui_strategy_config_apply",
            "ui_strategy_preset_save",
        },
        "portfolio": {
            "ui_sim_order_create",
            "ui_sim_order_cancel",
        },
        "flowhub": {
            "backfill_full_history",
            "backfill_data_type_history",
            "backfill_resume_run",
            "backfill_retry_failed_shards",
        },
    }
    HISTORY_MAX = 200
    SERVICE_PAGE_SIZE = 200
    SERVICE_MAX_FETCH = 5000

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
        self._validate_job_type(service, job_type)
        normalized_params = self._normalize_params(params)
        normalized_metadata = self._normalize_metadata(metadata)

        payload = service_payload if isinstance(service_payload, dict) else self._build_create_payload(service, job_type, normalized_params)
        request_payload_hash = self._hash_payload(payload)
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
            "metadata": normalized_metadata,
            "request_payload_hash": request_payload_hash,
        }
        await self._save_task_record(record)

        job_payload = await self._safe_get_service_job(service, service_job_id)
        normalized = self._normalize_job(service, job_payload or {"job_id": service_job_id}, task_job_id=task_job_id)
        normalized["job_type"] = normalized.get("job_type") or job_type
        normalized["metadata"] = {**record["metadata"], **(normalized.get("metadata") or {})}
        await self._append_history(
            task_job_id,
            "created",
            normalized,
            request_payload_hash=request_payload_hash,
            upstream_status=202,
        )
        return normalized

    async def list_task_jobs(
        self,
        service: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Dict[str, Any]:
        services = [service.lower()] if service else list(self.SERVICES)
        if service and services[0] not in self.SERVICES:
            raise ValueError(f"Unsupported service: {service}")
        jobs: List[Dict[str, Any]] = []
        errors: List[Dict[str, str]] = []

        for svc in services:
            try:
                raw_jobs = await self._list_service_jobs(svc, status=status)
                for raw_job in raw_jobs:
                    service_job_id = self._get_service_job_id(raw_job)
                    if not service_job_id:
                        continue
                    mapped_id = await self._find_task_job_id(svc, service_job_id)
                    task_job_id = mapped_id or f"{svc}:{service_job_id}"
                    normalized = self._normalize_job(svc, raw_job, task_job_id=task_job_id)
                    if mapped_id:
                        await self._append_history_if_changed(mapped_id, normalized)
                    if status and normalized.get("status") != self._normalize_status(status):
                        continue
                    jobs.append(normalized)
            except UpstreamServiceError as exc:
                errors.append(
                    {
                        "service": svc,
                        "upstream_status": exc.status,
                        "error": exc.error.get("message") or str(exc),
                    }
                )
            except Exception as exc:
                errors.append({"service": svc, "error": str(exc)})

        jobs.sort(
            key=lambda item: (
                self._as_timestamp(item.get("updated_at") or item.get("completed_at") or item.get("created_at")),
                str(item.get("id") or ""),
            ),
            reverse=True,
        )
        total = len(jobs)
        page = jobs[offset: offset + limit]
        return {
            "jobs": page,
            "total": total,
            "limit": limit,
            "offset": offset,
            "errors": errors,
        }

    async def _list_service_jobs(self, service: str, status: Optional[str]) -> List[Dict[str, Any]]:
        jobs: List[Dict[str, Any]] = []
        page_size = self.SERVICE_PAGE_SIZE
        svc_offset = 0

        while svc_offset < self.SERVICE_MAX_FETCH:
            payload = await self._request_service(
                service,
                "GET",
                "/api/v1/jobs",
                params={
                    "limit": page_size,
                    "offset": svc_offset,
                    **({"status": status} if status else {}),
                },
            )
            page = self._extract_jobs(payload)
            if not page:
                break
            jobs.extend(page)
            if len(page) < page_size:
                break
            svc_offset += page_size

        return jobs

    async def get_task_job(self, task_job_id: str) -> Dict[str, Any]:
        service, service_job_id, mapped_task_job_id = await self._resolve_task_job_id(task_job_id)
        payload = await self._request_service(service, "GET", f"/api/v1/jobs/{service_job_id}")
        normalized = self._normalize_job(service, self._extract_data(payload), task_job_id=mapped_task_job_id or task_job_id)
        if mapped_task_job_id:
            await self._append_history_if_changed(mapped_task_job_id, normalized)
        return normalized

    async def cancel_task_job(self, task_job_id: str) -> Dict[str, Any]:
        service, service_job_id, mapped_task_job_id = await self._resolve_task_job_id(task_job_id)
        await self._request_service(service, "DELETE", f"/api/v1/jobs/{service_job_id}")
        payload = await self._safe_get_service_job(service, service_job_id)
        normalized = self._normalize_job(
            service,
            payload or {"job_id": service_job_id, "status": "cancelled"},
            task_job_id=mapped_task_job_id or task_job_id,
        )
        if mapped_task_job_id:
            await self._append_history(mapped_task_job_id, "cancelled", normalized)
        return normalized

    async def get_task_job_history(self, task_job_id: str) -> Dict[str, Any]:
        try:
            service, service_job_id, mapped_task_job_id = await self._resolve_task_job_id(task_job_id)
        except Exception:
            return {"task_job_id": task_job_id, "history": []}

        resolved_id = mapped_task_job_id or task_job_id
        history = await self._get_history(resolved_id)
        if not history:
            payload = await self._safe_get_service_job(service, service_job_id)
            if payload:
                history.append({
                    "event": "snapshot",
                    "timestamp": self._utc_now(),
                    "job": self._normalize_job(service, payload, task_job_id=resolved_id),
                })

        return {"task_job_id": resolved_id, "history": history}

    async def _resolve_task_job_id(self, task_job_id: str) -> Tuple[str, str, Optional[str]]:
        record = await self._load_task_record(task_job_id)
        if record:
            return record["service"], record["service_job_id"], task_job_id

        if ":" in task_job_id:
            service, service_job_id = task_job_id.split(":", 1)
            service = service.lower()
            if service in self.SERVICES and service_job_id:
                return service, service_job_id, None

        raise ValueError(f"Unknown task_job_id: {task_job_id}")

    def _validate_job_type(self, service: str, job_type: str) -> None:
        value = (job_type or "").strip()
        if not value:
            raise ValueError("Missing required field: job_type")
        allowed = self.SERVICE_JOB_TYPES.get(service, set())
        if allowed and value not in allowed:
            raise ValueError(f"Unsupported job_type for {service}: {job_type}")

    @staticmethod
    def _normalize_params(params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if params is None:
            return {}
        if not isinstance(params, dict):
            raise ValueError("params must be a JSON object")
        return dict(params)

    @staticmethod
    def _normalize_metadata(metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if metadata is None:
            return {}
        if not isinstance(metadata, dict):
            raise ValueError("metadata must be a JSON object")
        return dict(metadata)

    def _build_create_payload(self, service: str, job_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
        if service == "flowhub":
            flowhub_payload = dict(params)
            flowhub_payload.setdefault("data_type", job_type)
            return flowhub_payload
        return {
            "job_type": job_type,
            "params": params,
        }

    @staticmethod
    def _normalize_upstream_error(payload: Any, status: int) -> Dict[str, Any]:
        if isinstance(payload, dict):
            message = (
                payload.get("error")
                or payload.get("message")
                or (payload.get("data") or {}).get("error")
                if isinstance(payload.get("data"), dict)
                else None
            )
            return {
                "status": status,
                "message": str(message or f"Upstream request failed with status {status}"),
                "raw": payload,
            }
        return {
            "status": status,
            "message": f"Upstream request failed with status {status}",
            "raw": payload,
        }

    @staticmethod
    def _hash_payload(payload: Dict[str, Any]) -> str:
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

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
                raise UpstreamServiceError(
                    service=service,
                    method=method,
                    path=path,
                    status=resp.status,
                    error=self._normalize_upstream_error(data, resp.status),
                )
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

    async def _find_task_job_id(self, service: str, service_job_id: str) -> Optional[str]:
        for task_job_id, record in self._brain_jobs.items():
            if record.get("service") == service and record.get("service_job_id") == service_job_id:
                return task_job_id

        redis = self._redis()
        if not redis:
            return None
        key = self._index_key(service, service_job_id)
        mapped = await redis.get(key)
        return mapped or None

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

    async def _append_history(
        self,
        task_job_id: str,
        event: str,
        normalized_job: Dict[str, Any],
        request_payload_hash: Optional[str] = None,
        upstream_status: Optional[int] = None,
    ) -> None:
        entry = {
            "event": event,
            "timestamp": self._utc_now(),
            "job": dict(normalized_job),
        }
        if request_payload_hash:
            entry["request_payload_hash"] = request_payload_hash
        if upstream_status is not None:
            entry["upstream_status"] = upstream_status
        self._history.setdefault(task_job_id, []).append(entry)

        redis = self._redis()
        if not redis:
            return
        key = self._history_key(task_job_id)
        await redis.rpush(key, json.dumps(entry, ensure_ascii=False))
        await redis.ltrim(key, -self.HISTORY_MAX, -1)

    async def _append_history_if_changed(self, task_job_id: str, normalized_job: Dict[str, Any]) -> None:
        history = await self._get_history(task_job_id)
        if not history:
            await self._append_history(task_job_id, "snapshot", normalized_job)
            return
        last_job = history[-1].get("job", {})
        if (
            last_job.get("status") != normalized_job.get("status")
            or last_job.get("progress") != normalized_job.get("progress")
            or last_job.get("updated_at") != normalized_job.get("updated_at")
        ):
            await self._append_history(task_job_id, "snapshot", normalized_job)

    async def _save_task_record(self, record: Dict[str, Any]) -> None:
        task_job_id = record["task_job_id"]
        self._brain_jobs[task_job_id] = dict(record)

        redis = self._redis()
        if not redis:
            return
        await redis.set(self._record_key(task_job_id), json.dumps(record, ensure_ascii=False))
        await redis.set(self._index_key(record["service"], record["service_job_id"]), task_job_id)

    async def _load_task_record(self, task_job_id: str) -> Optional[Dict[str, Any]]:
        local = self._brain_jobs.get(task_job_id)
        if isinstance(local, dict):
            return dict(local)

        redis = self._redis()
        if not redis:
            return None
        raw = await redis.get(self._record_key(task_job_id))
        if not raw:
            return None
        try:
            record = json.loads(raw)
        except Exception:
            return None
        if isinstance(record, dict):
            self._brain_jobs[task_job_id] = dict(record)
            return dict(record)
        return None

    async def _get_history(self, task_job_id: str) -> List[Dict[str, Any]]:
        local = self._history.get(task_job_id)
        if local:
            return [dict(item) for item in local]

        redis = self._redis()
        if not redis:
            return []
        rows = await redis.lrange(self._history_key(task_job_id), 0, -1)
        history: List[Dict[str, Any]] = []
        for row in rows:
            try:
                parsed = json.loads(row)
            except Exception:
                continue
            if isinstance(parsed, dict):
                history.append(parsed)
        if history:
            self._history[task_job_id] = list(history)
        return history

    def _redis(self):
        return self._app.get("redis")

    @staticmethod
    def _record_key(task_job_id: str) -> str:
        return f"brain:task_job:{task_job_id}"

    @staticmethod
    def _index_key(service: str, service_job_id: str) -> str:
        return f"brain:task_job:index:{service}:{service_job_id}"

    @staticmethod
    def _history_key(task_job_id: str) -> str:
        return f"brain:task_job:history:{task_job_id}"

    @staticmethod
    def _utc_now() -> str:
        return datetime.utcnow().isoformat() + "Z"

    @staticmethod
    def _as_timestamp(value: Any) -> float:
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return 0.0
        try:
            return float(text)
        except Exception:
            pass
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0.0
