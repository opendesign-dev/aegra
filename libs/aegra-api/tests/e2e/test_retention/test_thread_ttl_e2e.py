"""E2E tests for the thread TTL sweep predicate, executed on real PostgreSQL.

The predicate mixes a JSONB extract, a float cast, and ``make_interval``. Compiling
it proves nothing — Postgres types ``make_interval``'s ``mins`` as integer, so a
float in that slot compiles fine and then raises UndefinedFunction at runtime.
Only executing it against a real server catches that class of bug.
"""

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.services.thread_ttl_sweeper import _expired
from aegra_api.settings import DatabaseSettings, settings

from .._utils import elog

PREFIX = "e2e-ttl-"
DAY_MINUTES = 1440


def _minutes(days: float) -> dict[str, object]:
    return {"ttl": days * DAY_MINUTES, "strategy": "delete"}


def _database_url() -> str:
    """The URL the running server uses.

    Pytest doesn't load ``.env`` — only the CLI does, before starting the server —
    so ``settings.db`` would otherwise hold defaults that miss the compose
    credentials. Real environment variables still win, as in the CLI.
    """
    for parent in Path(__file__).resolve().parents:
        env_file = parent / ".env"
        if not env_file.is_file():
            continue
        for line in env_file.read_text(encoding="utf-8").splitlines():
            key, sep, value = line.partition("=")
            key = key.strip()
            if sep and key.startswith("POSTGRES_") and key not in os.environ:
                os.environ[key] = value.strip()
        break
    return DatabaseSettings().database_url


@pytest.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    """A short-lived engine, skipping the module when Postgres isn't reachable."""
    eng = create_async_engine(_database_url(), pool_pre_ping=True)
    try:
        async with eng.connect():
            pass
    except Exception as exc:
        await eng.dispose()
        pytest.skip(f"PostgreSQL not reachable from the test host: {type(exc).__name__}")
    try:
        yield eng
    finally:
        await eng.dispose()


@pytest.fixture
async def seed(engine: AsyncEngine):
    """Insert aged threads, yield a matcher for the sweep predicate, then clean up."""
    maker = async_sessionmaker(engine, expire_on_commit=False)
    created: list[str] = []

    async def _seed(cases: list[tuple[str, float, dict[str, object] | None]]) -> set[str]:
        now = datetime.now(UTC)
        async with maker() as session:
            for name, age_days, ttl in cases:
                thread_id = f"{PREFIX}{name}"
                created.append(thread_id)
                stamp = now - timedelta(days=age_days)
                session.add(
                    ThreadORM(
                        thread_id=thread_id,
                        user_id="e2e-ttl",
                        status="idle",
                        metadata_json={},
                        ttl=ttl,
                        created_at=stamp,
                        updated_at=stamp,
                    )
                )
            await session.commit()

        async with maker() as session:
            matched = await session.scalars(
                select(ThreadORM.thread_id).where(ThreadORM.thread_id.in_(created), _expired())
            )
            return {t.removeprefix(PREFIX) for t in matched.all()}

    try:
        yield _seed
    finally:
        async with maker() as session:
            await session.execute(delete(ThreadORM).where(ThreadORM.thread_id.in_(created)))
            await session.commit()


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_predicate_executes_on_postgres(seed) -> None:
    """Guards the make_interval signature: a float minutes value must not land in
    the integer ``mins`` slot, or every sweep tick dies with UndefinedFunction."""
    matched = await seed([("frac", 1.0, {"ttl": 0.5, "strategy": "delete"})])
    assert matched == {"frac"}


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_threads_without_ttl_use_the_global_default(seed) -> None:
    global_days = settings.checkpointer.CHECKPOINTER_TTL_MINUTES / DAY_MINUTES
    matched = await seed(
        [
            ("global-young", global_days / 2, None),
            ("global-old", global_days * 2, None),
        ]
    )

    elog("swept with no per-thread ttl", sorted(matched))
    assert matched == {"global-old"}


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_own_ttl_shortens_retention(seed) -> None:
    """A thread younger than the global default is still swept when its own ttl says so."""
    global_days = settings.checkpointer.CHECKPOINTER_TTL_MINUTES / DAY_MINUTES
    age = global_days / 4
    matched = await seed([("short", age, _minutes(age / 2)), ("control", age, None)])

    assert matched == {"short"}


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_own_ttl_extends_retention(seed) -> None:
    """The discriminating case: the global default would sweep this thread, and its
    own longer ttl must override that rather than merely agreeing with it."""
    global_days = settings.checkpointer.CHECKPOINTER_TTL_MINUTES / DAY_MINUTES
    age = global_days * 2
    matched = await seed([("long", age, _minutes(age * 2)), ("control", age, None)])

    elog("swept with a protective per-thread ttl", sorted(matched))
    assert matched == {"control"}
