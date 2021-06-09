import asyncio
from asyncio import AbstractEventLoop
from typing import Generator

import pytest
from _pytest.fixtures import FixtureRequest
from eventual.model import Event, EventPayload
from tortoise.contrib.test import finalizer, initializer

from eventual_tortoise import TortoiseEventSchedule, TortoiseIntegrityGuard
from eventual_tortoise.relation import ScheduledEventEntryRelation
from tests.model import SomethingHappened


@pytest.yield_fixture(scope="function")
def event_loop(request: FixtureRequest) -> Generator[AbstractEventLoop, None, None]:
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="function", autouse=True)
def initialize_tests(request: FixtureRequest) -> None:
    initializer(
        ["eventual_tortoise.relation"],
        db_url="sqlite://:memory:",
        app_label="default",
    )
    request.addfinalizer(finalizer)


@pytest.fixture()
def event() -> SomethingHappened:
    return SomethingHappened()


@pytest.fixture
def event_payload(event: Event) -> EventPayload:
    return EventPayload.from_event(event)


@pytest.fixture
async def event_entry_count() -> int:
    return await ScheduledEventEntryRelation.all().count()


@pytest.fixture
async def event_schedule(request: FixtureRequest) -> TortoiseEventSchedule:
    claim_duration = request.param if hasattr(request, "param") else 0
    return TortoiseEventSchedule(claim_duration=claim_duration)


@pytest.fixture
async def integrity_guard() -> TortoiseIntegrityGuard:
    return TortoiseIntegrityGuard()
