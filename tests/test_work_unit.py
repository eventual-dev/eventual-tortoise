from contextlib import suppress
from typing import Type

import pytest
from eventual import util
from eventual.abc.work_unit import InterruptWork
from eventual.model import EventPayload

from eventual_tortoise.relation import ScheduledEventEntryRelation
from eventual_tortoise.work_unit import TortoiseWorkUnit


async def add_event_to_schedule(event_payload: EventPayload) -> None:
    await ScheduledEventEntryRelation.create(
        event_id=event_payload.id,
        body=event_payload.body,
        claimed_at=util.tz_aware_utcnow(),
        due_after=util.tz_aware_utcnow(),
    )


@pytest.mark.parametrize("exc_cls", [ValueError, TypeError])
@pytest.mark.asyncio
async def test_exceptions_are_not_caught(
    exc_cls: Type[BaseException], event_payload: EventPayload
) -> None:
    with pytest.raises(exc_cls):
        async with TortoiseWorkUnit.create():
            await add_event_to_schedule(event_payload)
            raise exc_cls


@pytest.mark.asyncio
async def test_interrupt_work_is_caught(
    event_entry_count: int, event_payload: EventPayload
) -> None:
    async with TortoiseWorkUnit.create():
        await add_event_to_schedule(event_payload)
        raise InterruptWork


@pytest.mark.asyncio
async def test_commit_by_default(
    event_entry_count: int, event_payload: EventPayload
) -> None:
    async with TortoiseWorkUnit.create():
        await add_event_to_schedule(event_payload)

    count_after = await ScheduledEventEntryRelation.all().count()
    assert count_after == event_entry_count + 1


@pytest.mark.parametrize("exc_cls", [ValueError, TypeError, InterruptWork])
@pytest.mark.asyncio
async def test_no_commit_when_exception_is_raised(
    exc_cls: Type[BaseException], event_entry_count: int, event_payload: EventPayload
) -> None:
    with suppress(exc_cls):
        async with TortoiseWorkUnit.create():
            await add_event_to_schedule(event_payload)
            raise exc_cls

    count_after = await ScheduledEventEntryRelation.all().count()
    assert count_after == event_entry_count


@pytest.mark.asyncio
async def test_commit_means_committed_true(
    event_entry_count: int, event_payload: EventPayload
) -> None:
    async with TortoiseWorkUnit.create() as work_unit:
        await add_event_to_schedule(event_payload)

    count_after = await ScheduledEventEntryRelation.all().count()
    assert count_after == event_entry_count + 1

    assert work_unit.committed


@pytest.mark.asyncio
async def test_no_commit_means_committed_false(
    event_entry_count: int, event_payload: EventPayload
) -> None:
    async with TortoiseWorkUnit.create() as work_unit:
        await add_event_to_schedule(event_payload)
        raise InterruptWork

    count_after = await ScheduledEventEntryRelation.all().count()
    assert count_after == event_entry_count

    assert not work_unit.committed
