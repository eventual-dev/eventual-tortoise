import pytest
from eventual.abc.guarantee import Guarantee
from eventual.model import EventPayload
from tortoise.exceptions import IntegrityError

from eventual_tortoise import TortoiseIntegrityGuard
from eventual_tortoise.relation import DispatchedEventRelation, HandledEventRelation
from eventual_tortoise.work_unit import TortoiseWorkUnit

pytestmark = pytest.mark.asyncio


async def test_can_create_work_unit(
    integrity_guard: TortoiseIntegrityGuard,
) -> None:
    async with integrity_guard.create_work_unit() as work_unit:
        assert isinstance(work_unit, TortoiseWorkUnit)


@pytest.mark.parametrize("guarantee", tuple(Guarantee))
async def test_can_record_completion_with_guarantee(
    integrity_guard: TortoiseIntegrityGuard,
    event_payload: EventPayload,
    guarantee: Guarantee,
) -> None:
    count_handled = await HandledEventRelation.all().count()
    await integrity_guard.record_completion_with_guarantee(event_payload, guarantee)
    count_after = await HandledEventRelation.all().count()

    assert count_after == count_handled + 1


async def test_can_record_completion_with_guarantee_only_once(
    integrity_guard: TortoiseIntegrityGuard, event_payload: EventPayload
) -> None:
    with pytest.raises(IntegrityError):
        await integrity_guard.record_completion_with_guarantee(
            event_payload, Guarantee.AT_LEAST_ONCE
        )
        await integrity_guard.record_completion_with_guarantee(
            event_payload, Guarantee.AT_LEAST_ONCE
        )


async def test_can_record_dispatch_attempt(
    integrity_guard: TortoiseIntegrityGuard, event_payload: EventPayload
) -> None:
    count_dispatched = await DispatchedEventRelation.all().count()
    await integrity_guard.record_dispatch_attempt(event_payload)

    count_after = await DispatchedEventRelation.all().count()
    assert count_after == count_dispatched + 1


async def test_can_record_dispatch_attempt_multiple_times(
    integrity_guard: TortoiseIntegrityGuard, event_payload: EventPayload
) -> None:
    count_dispatched = await DispatchedEventRelation.all().count()

    await integrity_guard.record_dispatch_attempt(event_payload)
    await integrity_guard.record_dispatch_attempt(event_payload)

    count_after = await DispatchedEventRelation.all().count()
    assert count_after == count_dispatched + 2


@pytest.mark.parametrize("guarantee", tuple(Guarantee))
async def test_can_check_if_dispatch_forbidden(
    integrity_guard: TortoiseIntegrityGuard,
    event_payload: EventPayload,
    guarantee: Guarantee,
) -> None:
    event_id = event_payload.id
    assert not await integrity_guard.is_dispatch_forbidden(event_id)
    await integrity_guard.record_completion_with_guarantee(event_payload, guarantee)
    assert await integrity_guard.is_dispatch_forbidden(event_id)
