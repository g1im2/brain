import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from task_orchestrator import TaskOrchestrator, UpstreamServiceError


@pytest.mark.asyncio
async def test_get_task_job_falls_back_to_brain_record_on_upstream_error(monkeypatch):
    orchestrator = TaskOrchestrator({"redis": None})

    async def _resolve(_task_job_id):
        return "macro", "svc-job-1", "brain-job-1"

    async def _request(*_args, **_kwargs):
        raise UpstreamServiceError(
            service="macro",
            method="GET",
            path="/api/v1/jobs/svc-job-1",
            status=404,
            error={"message": "not found"},
        )

    async def _load(_task_job_id):
        return {"job_type": "ui_macro_cycle_freeze", "metadata": {"source": "unit"}}

    monkeypatch.setattr(orchestrator, "_resolve_task_job_id", _resolve)
    monkeypatch.setattr(orchestrator, "_request_service", _request)
    monkeypatch.setattr(orchestrator, "_load_task_record", _load)

    result = await orchestrator.get_task_job("brain-job-1")

    assert result["id"] == "brain-job-1"
    assert result["service"] == "macro"
    assert result["service_job_id"] == "svc-job-1"
    assert result["status"] == "queued"
    assert result["job_type"] == "ui_macro_cycle_freeze"
    assert result["error"]["upstream_status"] == 404


@pytest.mark.asyncio
async def test_cancel_task_job_tolerates_upstream_404(monkeypatch):
    orchestrator = TaskOrchestrator({"redis": None})

    async def _resolve(_task_job_id):
        return "macro", "svc-job-2", "brain-job-2"

    async def _request(*_args, **_kwargs):
        raise UpstreamServiceError(
            service="macro",
            method="DELETE",
            path="/api/v1/jobs/svc-job-2",
            status=404,
            error={"message": "not found"},
        )

    async def _safe_get(*_args, **_kwargs):
        return None

    monkeypatch.setattr(orchestrator, "_resolve_task_job_id", _resolve)
    monkeypatch.setattr(orchestrator, "_request_service", _request)
    monkeypatch.setattr(orchestrator, "_safe_get_service_job", _safe_get)

    result = await orchestrator.cancel_task_job("brain-job-2")

    assert result["id"] == "brain-job-2"
    assert result["service"] == "macro"
    assert result["service_job_id"] == "svc-job-2"
    assert result["status"] == "cancelled"


@pytest.mark.asyncio
async def test_find_task_job_id_returns_none_when_redis_get_fails(monkeypatch):
    orchestrator = TaskOrchestrator({"redis": None})

    class _BadRedis:
        async def get(self, _key):
            raise RuntimeError("loop mismatch")

    monkeypatch.setattr(orchestrator, "_redis", lambda: _BadRedis())
    mapped = await orchestrator._find_task_job_id("macro", "svc-job-3")
    assert mapped is None


@pytest.mark.asyncio
async def test_load_task_record_returns_none_when_redis_get_fails(monkeypatch):
    orchestrator = TaskOrchestrator({"redis": None})

    class _BadRedis:
        async def get(self, _key):
            raise RuntimeError("loop mismatch")

    monkeypatch.setattr(orchestrator, "_redis", lambda: _BadRedis())
    record = await orchestrator._load_task_record("brain-job-3")
    assert record is None
