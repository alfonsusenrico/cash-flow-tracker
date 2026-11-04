"""The Sesi uji replay tooling (notification-test-session D7, tasks 4.1-4.3). Synthetic data only."""
import importlib.util
import json
from pathlib import Path

import pytest

from test_duplicate_arrivals import GOPAY, GOPAY_CREDIT, JAGO, JAGO_CREDIT, MYBCA, MYBCA_DEBIT, add_link, event
from test_notification_processing_database import ingest, notification_owner  # noqa: F401 - fixture
from test_notification_test_sessions import add_session

VERIFY = Path(__file__).resolve().parents[2] / "scripts" / "verify"


def load(name):
    spec = importlib.util.spec_from_file_location(name, VERIFY / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


exporter = load("export_test_session")
replayer = load("replay_test_session")
scorer = load("score_test_session")


def test_the_exporter_refuses_a_path_inside_the_repository(tmp_path):
    repository = VERIFY.parents[1]
    with pytest.raises(SystemExit):
        exporter.refuse_repository_path(repository / "export.json", repository)
    with pytest.raises(SystemExit):
        exporter.refuse_repository_path(repository / "research" / "export.json", repository)
    exporter.refuse_repository_path(tmp_path / "export.json", repository)


@pytest.mark.parametrize("url", [
    "postgresql://ledger:secret@db.example.com:5432/ledger",
    "postgresql://ledger:secret@10.0.0.5:5432/ledger",
    "postgresql://ledger:secret@ledger_db:5432/ledger",
])
def test_the_replay_refuses_a_non_loopback_database(url):
    with pytest.raises(SystemExit):
        replayer.refuse_remote(url)


def test_the_replay_accepts_loopback_databases():
    for url in ("postgresql://ledger:x@127.0.0.1:55642/replay", "postgresql://ledger:x@localhost/replay"):
        replayer.refuse_remote(url)


def export_session(client, owner, *, cases=("1", "1", "1")):
    """Captures the 1 Nov sequence in a session and exports it, as the owner's session day would."""
    from app.db.pool import db_conn

    session_id = add_session(owner)
    results = ingest(client, event(GOPAY_CREDIT, GOPAY, 0), event(JAGO_CREDIT, JAGO, 0.13), event(MYBCA_DEBIT, MYBCA, 3.4))["results"]
    with db_conn() as conn:
        for result, case in zip(results, cases):
            conn.execute("UPDATE notification_events SET test_case = %s WHERE id = %s", (case, result["event_id"]))
        conn.commit()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            data = exporter.export(cur, owner["user_id"], session_id)
        conn.rollback()
    # The replay reads the export as JSON, exactly as the file on disk.
    return json.loads(json.dumps(data))


@pytest.fixture
def restored_settings(monkeypatch):
    """The replay switches the processing settings; restore them for the next test."""
    from app.services import notification_processing as processing
    monkeypatch.setattr(processing, "settings", processing.settings)


def test_an_export_holds_the_session_and_the_structure(client, notification_owner):
    data = export_session(client, notification_owner)
    assert data["counts"]["events"] == 3 and data["counts"]["accounts"] == 7
    assert [row["test_case"] for row in data["events"]] == ["1", "1", "1"]
    assert {row["package_name"] for row in data["events"]} == {GOPAY, JAGO, MYBCA}


def test_a_replay_books_the_sequence_and_holds_the_mirror_without_a_link(client, notification_owner, restored_settings):
    data = export_session(client, notification_owner)
    result = replayer.replay(data, drop_apps=set(), ai=False)
    assert result["failures"] == []
    assert result["cases"]["1"] == {"events": 3, "movements": 1, "income": 0, "expense": 0, "held": 1, "skipped": 0,
                                    "review": 0, "ignored": 0, "questions": 0}
    report = scorer.score(result, {"cases": {"1": {"movements": 1, "held": 1, "income": 0}}})
    assert (report["passed"], report["failed"]) == (1, 0)


def test_a_replay_with_the_account_link_skips_the_mirror(client, notification_owner, restored_settings):
    add_link(notification_owner)
    data = export_session(client, notification_owner)
    result = replayer.replay(data, drop_apps=set(), ai=False)
    assert (result["cases"]["1"]["movements"], result["cases"]["1"]["skipped"], result["cases"]["1"]["held"]) == (1, 1, 0)


def test_dropping_an_app_simulates_a_one_sided_report(client, notification_owner, restored_settings):
    data = export_session(client, notification_owner)
    result = replayer.replay(data, drop_apps={JAGO}, ai=False)
    assert result["dropped_apps"] == [JAGO]
    assert result["cases"]["1"]["events"] == 2 and result["cases"]["1"]["held"] == 0
    assert all(item["package_name"] != JAGO for item in result["events"])


def test_the_replay_leaves_no_synthetic_owner_behind(client, notification_owner, restored_settings):
    from app.db.pool import db_conn

    data = export_session(client, notification_owner)
    with db_conn() as conn:
        before = conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]
    replayer.replay(data, drop_apps=set(), ai=False)
    with db_conn() as conn:
        assert conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"] == before


def test_the_scorer_reports_differences_and_missing_cases():
    replay = {"cases": {"1": {"movements": 1, "held": 1}}}
    report = scorer.score(replay, {"cases": {"1": {"movements": 1, "held": 0}, "2": {"movements": 1}}})
    assert report["passed"] == 0 and report["failed"] == 2
    first, second = report["results"]
    assert first["differences"] == {"held": {"expected": 0, "actual": 1}}
    assert second["missing"] is True
