import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from handlers.ui_bff import UIBffHandler


def test_ui_bff_mutation_route_mapping():
    route, _ = UIBffHandler._match_route("POST", "/api/v1/ui/macro-cycle/freeze")
    assert route is not None
    assert route.service == "macro"
    assert route.job_type == "ui_macro_cycle_freeze"
    assert route.is_mutation is True


def test_ui_bff_read_route_mapping():
    route, params = UIBffHandler._match_route("GET", "/api/v1/ui/research/subjects/abc-123")
    assert route is not None
    assert route.service == "execution"
    assert route.is_mutation is False
    assert params["subject_id"] == "abc-123"


def test_ui_bff_extended_macro_route_mapping():
    route, params = UIBffHandler._match_route("POST", "/api/v1/ui/macro-cycle/snap-001/mark-seen")
    assert route is not None
    assert route.service == "macro"
    assert route.is_mutation is True
    assert route.job_type == "ui_macro_cycle_mark_seen"
    assert params["snapshot_id"] == "snap-001"


def test_ui_bff_extended_strategy_route_mapping():
    route, params = UIBffHandler._match_route("GET", "/api/v1/ui/strategy/reports/rpt-001/analysis")
    assert route is not None
    assert route.service == "execution"
    assert route.is_mutation is False
    assert params["report_id"] == "rpt-001"
