import asyncio
from dataclasses import replace
from threading import Event
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.parametrize("enabled,key,model,reasoning_effort,dry_run,expect_client,expect_worker_provider", [
    (False, "", "gpt-5.6-luna", "low", False, False, False),
    (True, "", "gpt-5.6-luna", "low", False, False, False),
    (True, "synthetic-test-credential", "gpt-5.6-terra", "low", False, False, False),
    (True, "synthetic-test-credential", "gpt-5.6-luna", "medium", False, False, False),
    (True, "synthetic-test-credential", "gpt-5.6-luna", "low", False, True, True),
    (False, "synthetic-test-credential", "gpt-5.6-luna", "low", True, True, False),
])
def test_lifespan_starts_only_permitted_inference_and_closes_in_order(monkeypatch, enabled, key, model, reasoning_effort, dry_run, expect_client, expect_worker_provider):
    import app.main as main

    events = []
    monkeypatch.setattr(main, "settings", replace(main.settings, notification_ai_enabled=enabled,
                       openai_api_key=key, notification_ai_model=model,
                       notification_ai_reasoning_effort=reasoning_effort,
                       app_env="development" if dry_run else "production",
                       notification_ai_dry_run_enabled=dry_run))
    monkeypatch.setattr(main, "open_db_pool", lambda: events.append("open"))
    monkeypatch.setattr(main, "close_db_pool", lambda: events.append("pool_closed"))
    client = MagicMock()
    client.aclose = AsyncMock(side_effect=lambda: events.append("client_closed"))
    factory = MagicMock(return_value=client)
    monkeypatch.setattr(main.httpx, "AsyncClient", factory)
    provider_factory = MagicMock(return_value=object())
    monkeypatch.setattr(main, "OpenAINotificationProvider", provider_factory)

    async def idle():
        events.append("background_started")
        await asyncio.Event().wait()

    async def worker(provider):
        assert (provider is not None) == expect_worker_provider
        events.append("worker_started")
        try:
            await idle()
        finally:
            events.append("worker_stopped")

    monkeypatch.setattr(main, "_daily_price_sync_loop", idle)
    monkeypatch.setattr(main, "_ledger_matching_loop", idle)
    monkeypatch.setattr(main, "_plan_link_request_loop", idle)
    monkeypatch.setattr(main, "notification_worker", worker)

    async def run():
        async with main.lifespan(main.app):
            await asyncio.sleep(0)

    asyncio.run(run())
    assert factory.called == expect_client
    if dry_run and not enabled:
        assert "worker_started" not in events
        assert "background_started" not in events
        assert events.index("client_closed") < events.index("pool_closed")
        return
    if expect_client:
        assert factory.call_args.kwargs["trust_env"] is False
        assert events.index("worker_stopped") < events.index("client_closed") < events.index("pool_closed")
    else:
        assert events.index("worker_stopped") < events.index("pool_closed")
    # Price sync and salary detection only; no background task posts scheduled transactions.
    assert events.count("background_started") == 3


def test_shutdown_waits_for_in_flight_database_operation():
    from app.services.notification_processing import database_operation

    started, finish = Event(), Event()
    completed = []

    def database_work():
        started.set()
        assert finish.wait(timeout=5)
        completed.append(True)

    async def run():
        task = asyncio.create_task(database_operation(database_work))
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert completed == [True]


@pytest.mark.parametrize("fails", [False, True])
def test_daily_price_sync_waits_a_day_after_success_or_failure(monkeypatch, fails):
    import app.main as main
    waits = []
    connection = MagicMock()
    monkeypatch.setattr(main, "db_conn", lambda: connection)
    sync = AsyncMock(side_effect=RuntimeError("synthetic failure") if fails else None)
    monkeypatch.setattr(main, "sync_all_tracked_prices", sync)

    async def sleep(delay):
        waits.append(delay)
        if delay == 86400:
            raise asyncio.CancelledError

    monkeypatch.setattr(main.asyncio, "sleep", sleep)
    asyncio.run(main._daily_price_sync_loop())
    assert waits == [10, 86400]
    assert sync.await_count == 1


def test_no_scheduled_posting_loop_remains():
    import app.main as main
    import app.routers.recurring as recurring

    assert not hasattr(main, "_recurring_scheduler_loop")
    assert not hasattr(recurring, "process_due_recurring_rules")


def test_salary_detection_pages_quickly_then_waits_a_minute(monkeypatch):
    import app.main as main
    waits = []
    cursors = iter(["next-owner", None])
    monkeypatch.setattr(main, "settings", replace(main.settings, plan_auto_link_enabled=False))
    monkeypatch.setattr(main, "process_salary_sources", lambda cursor: next(cursors))

    async def sleep(delay):
        waits.append(delay)
        if delay == 60:
            raise asyncio.CancelledError

    monkeypatch.setattr(main.asyncio, "sleep", sleep)
    asyncio.run(main._ledger_matching_loop())
    assert waits == [15, 5, 60]


def test_plan_sweep_shares_the_periodic_loop_and_pages_independently(monkeypatch):
    import app.main as main
    waits = []
    salary = iter([None, None, None])
    plans = iter(["next-owner", None, None])
    seen = []
    monkeypatch.setattr(main, "settings", replace(main.settings, plan_auto_link_enabled=True))
    monkeypatch.setattr(main, "process_salary_sources", lambda cursor: next(salary))

    def sweep(cursor):
        seen.append(cursor)
        return next(plans)

    monkeypatch.setattr(main, "process_due_plan_links", sweep)

    async def sleep(delay):
        waits.append(delay)
        if delay == 60:
            raise asyncio.CancelledError

    monkeypatch.setattr(main.asyncio, "sleep", sleep)
    asyncio.run(main._ledger_matching_loop())
    assert seen == [None, "next-owner"]
    assert waits == [15, 5, 60]


def test_plan_link_requests_wake_the_request_loop(monkeypatch):
    import app.main as main
    import app.services.plan_occurrences as plans

    monkeypatch.setattr(plans, "settings", replace(plans.settings, plan_auto_link_enabled=True))
    linked = []

    def link():
        linked.append(plans.take_plan_link_requests())
        raise asyncio.CancelledError

    monkeypatch.setattr(main, "link_requested_plans", link)

    async def run():
        task = asyncio.create_task(main._plan_link_request_loop())
        await asyncio.sleep(0)
        await asyncio.to_thread(plans.request_plan_linking, "owner-a")
        await asyncio.wait_for(task, 2)

    asyncio.run(run())
    assert linked == [["owner-a"]]
    assert plans._waker is None
