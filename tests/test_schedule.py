import datetime as dt
from typing import List

import pytest
from eventual import util
from eventual.model import EventPayload

from eventual_tortoise import TortoiseEventSchedule
from eventual_tortoise.relation import ScheduledEventEntryRelation
from eventual_tortoise.work_unit import TortoiseWorkUnit

pytestmark = pytest.mark.asyncio


async def test_can_create_work_unit(event_schedule: TortoiseEventSchedule) -> None:
    async with event_schedule.create_work_unit() as work_unit:
        assert isinstance(work_unit, TortoiseWorkUnit)


async def test_add_event_entry(
    event_schedule: TortoiseEventSchedule,
    event_entry_count: int,
    event_payload: EventPayload,
) -> None:
    await event_schedule.add_claimed_event_entry(event_payload)
    count_after = await ScheduledEventEntryRelation.all().count()
    assert count_after == event_entry_count + 1
    assert not await event_schedule.is_event_entry_claimed(event_payload.id)


async def test_close_event_entry(
    event_schedule: TortoiseEventSchedule, event_payload: EventPayload
) -> None:
    await event_schedule.add_claimed_event_entry(event_payload)
    count_closed = await ScheduledEventEntryRelation.filter(closed=True).count()

    await event_schedule.close_event_entry(event_payload.id)
    count_after = await ScheduledEventEntryRelation.filter(closed=True).count()

    assert count_after == count_closed + 1
    assert await event_schedule.is_event_entry_closed(event_payload.id)


async def _collect_every_open_unclaimed_event_entry_due_now(
    event_schedule: TortoiseEventSchedule,
) -> List[EventPayload]:
    event_payload_seq = []
    async for event_entry_payload in event_schedule.every_open_unclaimed_event_entry_due_now():
        event_payload_seq.append(event_entry_payload)
    return event_payload_seq


async def test_every_open_unclaimed_event_entry_due_now(
    event_schedule: TortoiseEventSchedule, event_payload: EventPayload
) -> None:
    await event_schedule.add_claimed_event_entry(event_payload)
    event_payload_seq = await _collect_every_open_unclaimed_event_entry_due_now(
        event_schedule
    )

    assert len(event_payload_seq) == 1
    event_entry_payload, *_ = event_payload_seq

    assert event_payload.id == event_entry_payload.id
    assert event_payload.occurred_on == event_entry_payload.occurred_on
    assert event_payload.subject == event_entry_payload.subject


@pytest.mark.parametrize("event_schedule", [3600], indirect=True)
async def test_skip_claimed(
    event_schedule: TortoiseEventSchedule, event_payload: EventPayload
) -> None:
    await event_schedule.add_claimed_event_entry(event_payload)
    event_payload_seq = await _collect_every_open_unclaimed_event_entry_due_now(
        event_schedule
    )

    assert len(event_payload_seq) == 0


async def test_skip_closed(
    event_schedule: TortoiseEventSchedule, event_payload: EventPayload
) -> None:
    await event_schedule.add_claimed_event_entry(event_payload)
    await event_schedule.close_event_entry(event_payload.id)

    event_payload_seq = await _collect_every_open_unclaimed_event_entry_due_now(
        event_schedule
    )

    assert len(event_payload_seq) == 0


async def test_skip_not_yet_due(
    event_schedule: TortoiseEventSchedule, event_payload: EventPayload
) -> None:
    await event_schedule.add_claimed_event_entry(
        event_payload, due_after=util.tz_aware_utcnow() + dt.timedelta(days=1)
    )

    event_payload_seq = await _collect_every_open_unclaimed_event_entry_due_now(
        event_schedule
    )

    assert len(event_payload_seq) == 0
