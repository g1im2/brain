import sys
from pathlib import Path
from types import SimpleNamespace
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from handlers.ui_bff import UIBffHandler


def test_assess_requirement_uses_task_last_status_as_ready_fallback():
    handler = UIBffHandler()
    now_ts = 1771250000.0

    row = handler._assess_requirement_status(
        page_key="macro_cycle",
        page_label="宏观与周期",
        data_type="price_index_data",
        data_label="价格指数",
        jobs_by_type={},
        pipelines_by_type={
            "price_index_data": [
                {
                    "task_id": "task-price-index",
                    "enabled": True,
                    "schedule_type": "cron",
                    "schedule_value": "10 19 1 * *",
                    "next_run_at": "2026-03-01T19:10:00+08:00",
                    "last_status": "succeeded",
                    "last_job_id": "job-abc",
                    "last_run_at": 1771247650.0,
                }
            ]
        },
        now_ts=now_ts,
    )

    assert row["state"] == "ready"
    assert row["latest_job_status"] == "succeeded"
    assert row["latest_job_id"] == "job-abc"


def test_assess_requirement_uses_task_last_status_as_fetching_fallback():
    handler = UIBffHandler()
    now_ts = 1771250000.0

    row = handler._assess_requirement_status(
        page_key="macro_cycle",
        page_label="宏观与周期",
        data_type="macro_calendar_data",
        data_label="宏观日历",
        jobs_by_type={},
        pipelines_by_type={
            "macro_calendar_data": [
                {
                    "task_id": "task-macro-calendar",
                    "enabled": True,
                    "schedule_type": "cron",
                    "schedule_value": "30 7 * * *",
                    "next_run_at": "2026-02-17T07:30:00+08:00",
                    "last_status": "running",
                    "last_job_id": "job-running",
                    "last_run_at": 1771249000.0,
                }
            ]
        },
        now_ts=now_ts,
    )

    assert row["state"] == "fetching"
    assert row["running_job_count"] == 1
    assert row["latest_job_status"] == "running"


def test_assess_requirement_marks_no_data_when_success_but_no_saved_records():
    handler = UIBffHandler()
    now_ts = 1771250000.0

    row = handler._assess_requirement_status(
        page_key="macro_cycle",
        page_label="宏观与周期",
        data_type="macro_calendar_data",
        data_label="宏观日历",
        jobs_by_type={
            "macro_calendar_data": [
                {
                    "id": "job-latest",
                    "status": "succeeded",
                    "updated_at": 1771249999.0,
                    "result": {"success": True, "records_count": 0, "saved_count": 0},
                },
                {
                    "id": "job-previous",
                    "status": "succeeded",
                    "updated_at": 1771249000.0,
                    "result": {"success": True, "records_count": 0, "saved_count": 0},
                },
            ]
        },
        pipelines_by_type={},
        now_ts=now_ts,
    )

    assert row["state"] == "no_data"
    assert row["latest_saved_count"] == 0
    assert row["latest_records_count"] == 0
    assert row["history_has_data"] is False


def test_assess_requirement_stays_ready_when_history_has_non_zero_records():
    handler = UIBffHandler()
    now_ts = 1771250000.0

    row = handler._assess_requirement_status(
        page_key="macro_cycle",
        page_label="宏观与周期",
        data_type="price_index_data",
        data_label="价格指数",
        jobs_by_type={
            "price_index_data": [
                {
                    "id": "job-latest-zero",
                    "status": "succeeded",
                    "updated_at": 1771249999.0,
                    "result": {"success": True, "records_count": 0, "saved_count": 0},
                },
                {
                    "id": "job-history-has-data",
                    "status": "succeeded",
                    "updated_at": 1771248000.0,
                    "result": {"success": True, "records_count": 8, "saved_count": 8},
                },
            ]
        },
        pipelines_by_type={},
        now_ts=now_ts,
    )

    assert row["state"] == "ready"
    assert row["history_has_data"] is True


def test_assess_requirement_manual_trigger_policy_fields():
    handler = UIBffHandler()
    now_ts = 1771250000.0

    ready_row = handler._assess_requirement_status(
        page_key="macro_cycle",
        page_label="宏观与周期",
        data_type="price_index_data",
        data_label="价格指数",
        jobs_by_type={
            "price_index_data": [
                {"id": "job-ok", "status": "succeeded", "updated_at": 1771249999.0, "result": {"saved_count": 12}}
            ]
        },
        pipelines_by_type={},
        now_ts=now_ts,
    )
    assert ready_row["manual_trigger_allowed"] is False

    failed_row = handler._assess_requirement_status(
        page_key="macro_cycle",
        page_label="宏观与周期",
        data_type="money_supply_data",
        data_label="货币供应",
        jobs_by_type={
            "money_supply_data": [
                {"id": "job-failed", "status": "failed", "updated_at": 1771249999.0}
            ]
        },
        pipelines_by_type={},
        now_ts=now_ts,
    )
    assert failed_row["manual_trigger_allowed"] is True
    assert isinstance(failed_row.get("manual_trigger_reason"), str)


@pytest.mark.asyncio
async def test_readiness_trigger_rejects_ready_state():
    handler = UIBffHandler()

    async def fake_readiness(_request):
        return {
            "items": [
                {
                    "id": "macro_cycle:price_index_data",
                    "kind": "dataset",
                    "data_type": "price_index_data",
                    "state": "ready",
                }
            ]
        }

    handler._build_data_readiness_payload = fake_readiness  # type: ignore[method-assign]
    request = SimpleNamespace()
    resp = await handler._handle_system_readiness_item_trigger(request, "macro_cycle:price_index_data")
    assert resp.status == 409
    assert b"READINESS_TRIGGER_NOT_ALLOWED" in resp.body


@pytest.mark.asyncio
async def test_readiness_payload_real_data_override_keeps_manual_policy_consistent():
    handler = UIBffHandler()
    handler.DATA_READINESS_REQUIREMENTS = {
        "macro_cycle": {
            "label": "宏观与周期",
            "data_types": [{"data_type": "price_index_data", "label": "价格指数"}],
        }
    }

    class _FakeOrchestrator:
        async def list_task_jobs(self, **_kwargs):
            return {
                "jobs": [
                    {
                        "job_type": "price_index_data",
                        "status": "succeeded",
                        "updated_at": 1771249999.0,
                        "result": {"success": True, "saved_count": 0, "records_count": 0},
                    }
                ]
            }

    handler.get_app_component = lambda _req, _name: _FakeOrchestrator()  # type: ignore[method-assign]

    async def fake_list_tasks(_request, **_kwargs):
        return []

    async def fake_ensure(_request, tasks=None):
        return tasks or [], {"errors": []}

    async def fake_freshness():
        return {
            "price_index_data": {
                "latest_date": "2026-02-27",
                "rows_on_latest": 12,
                "table": "macro_indicator_raw",
            }
        }

    async def fake_page_checks(_request, _items):
        return []

    handler._list_flowhub_tasks = fake_list_tasks  # type: ignore[method-assign]
    handler._ensure_macro_cycle_default_pipelines = fake_ensure  # type: ignore[method-assign]
    handler._load_readiness_real_data_freshness = fake_freshness  # type: ignore[method-assign]
    handler._build_data_page_checks = fake_page_checks  # type: ignore[method-assign]

    payload = await handler._build_data_readiness_payload(SimpleNamespace())
    item = payload["items"][0]
    assert item["state"] == "ready"
    assert item["manual_trigger_allowed"] is False
    assert item["manual_trigger_reason"] == "数据已准备好，无需手动触发。"


@pytest.mark.asyncio
async def test_readiness_trigger_runs_existing_pipeline_task():
    handler = UIBffHandler()
    calls = {"task_id": ""}

    async def fake_readiness(_request):
        return {
            "items": [
                {
                    "id": "macro_cycle:price_index_data",
                    "kind": "dataset",
                    "data_type": "price_index_data",
                    "state": "failed",
                    "pipeline_task_id": "task-price-index",
                }
            ]
        }

    async def fake_run(_request, task_id):
        calls["task_id"] = task_id
        return {"job_id": "job-run-001"}

    handler._build_data_readiness_payload = fake_readiness  # type: ignore[method-assign]
    handler._run_flowhub_pipeline_task = fake_run  # type: ignore[method-assign]
    request = SimpleNamespace()
    resp = await handler._handle_system_readiness_item_trigger(request, "macro_cycle:price_index_data")
    assert resp.status == 200
    assert calls["task_id"] == "task-price-index"
    assert b"pipeline_run" in resp.body
    assert b"job-run-001" in resp.body


@pytest.mark.asyncio
async def test_readiness_trigger_auto_create_for_missing_task():
    handler = UIBffHandler()

    async def fake_readiness(_request):
        return {
            "items": [
                {
                    "id": "market_snapshot:market_flow_data",
                    "kind": "dataset",
                    "data_type": "market_flow_data",
                    "state": "missing_task",
                    "pipeline_task_id": "",
                }
            ]
        }

    async def fake_create_and_run(_request, data_type):
        assert data_type == "market_flow_data"
        return "task-market-flow", {"job_id": "job-run-888"}, {"name": "模板·市场快照·市场资金流"}

    handler._build_data_readiness_payload = fake_readiness  # type: ignore[method-assign]
    handler._create_and_run_flowhub_pipeline = fake_create_and_run  # type: ignore[method-assign]
    request = SimpleNamespace()
    resp = await handler._handle_system_readiness_item_trigger(request, "market_snapshot:market_flow_data")
    assert resp.status == 200
    assert b"auto_created_then_run" in resp.body
    assert b"task-market-flow" in resp.body
    assert b"job-run-888" in resp.body


@pytest.mark.asyncio
async def test_readiness_trigger_returns_404_when_item_not_found():
    handler = UIBffHandler()

    async def fake_readiness(_request):
        return {"items": []}

    handler._build_data_readiness_payload = fake_readiness  # type: ignore[method-assign]
    request = SimpleNamespace()
    resp = await handler._handle_system_readiness_item_trigger(request, "macro_cycle:price_index_data")
    assert resp.status == 404
    assert b"READINESS_ITEM_NOT_FOUND" in resp.body


@pytest.mark.asyncio
async def test_readiness_trigger_rejects_page_row():
    handler = UIBffHandler()

    async def fake_readiness(_request):
        return {
            "items": [
                {
                    "id": "page:macro_cycle",
                    "kind": "page",
                    "state": "failed",
                }
            ]
        }

    handler._build_data_readiness_payload = fake_readiness  # type: ignore[method-assign]
    request = SimpleNamespace()
    resp = await handler._handle_system_readiness_item_trigger(request, "page:macro_cycle")
    assert resp.status == 409
    assert b"READINESS_TRIGGER_NOT_ALLOWED" in resp.body


@pytest.mark.asyncio
async def test_readiness_trigger_returns_500_when_auto_create_fails():
    handler = UIBffHandler()

    async def fake_readiness(_request):
        return {
            "items": [
                {
                    "id": "market_snapshot:market_flow_data",
                    "kind": "dataset",
                    "data_type": "market_flow_data",
                    "state": "missing_task",
                    "pipeline_task_id": "",
                }
            ]
        }

    async def fake_create_and_run(_request, _data_type):
        raise RuntimeError("create failed")

    handler._build_data_readiness_payload = fake_readiness  # type: ignore[method-assign]
    handler._create_and_run_flowhub_pipeline = fake_create_and_run  # type: ignore[method-assign]
    request = SimpleNamespace()
    resp = await handler._handle_system_readiness_item_trigger(request, "market_snapshot:market_flow_data")
    assert resp.status == 500
    assert b"READINESS_TRIGGER_INTERNAL_ERROR" in resp.body
