import sys
from pathlib import Path

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
