import datetime as dt
import uuid
from typing import AsyncContextManager, AsyncGenerator, Iterable, Optional

from eventual import util
from eventual.abc.schedule import EventSchedule
from eventual.model import EventPayload

from .relation import ScheduledEventEntryRelation
from .work_unit import TortoiseWorkUnit


class TortoiseEventSchedule(EventSchedule[TortoiseWorkUnit]):
    def create_work_unit(self) -> AsyncContextManager[TortoiseWorkUnit]:
        return TortoiseWorkUnit.create()

    async def add_claimed_event_entry(
        self, event_payload: EventPayload, due_after: Optional[dt.datetime] = None
    ) -> None:
        utc_now = util.tz_aware_utcnow()
        event_id = event_payload.id
        if due_after is None:
            due_after = utc_now

        await ScheduledEventEntryRelation.create(
            event_id=event_id,
            body=event_payload.body,
            claimed_at=utc_now,
            due_after=due_after,
        )

    async def is_event_entry_claimed(self, event_id: uuid.UUID) -> bool:
        event = await ScheduledEventEntryRelation.filter(event_id=event_id).get()
        return event.claimed_at >= util.tz_aware_utcnow() - dt.timedelta(
            seconds=self.claim_duration
        )

    async def every_open_unclaimed_event_entry_due_now(
        self,
    ) -> AsyncGenerator[EventPayload, None]:
        utc_now = util.tz_aware_utcnow()

        async with TortoiseWorkUnit.create():
            event_entry_iter: Iterable[ScheduledEventEntryRelation] = (
                await ScheduledEventEntryRelation.select_for_update(skip_locked=True)
                .filter(
                    closed=False,
                    due_after__lte=utc_now,
                    claimed_at__lt=utc_now - dt.timedelta(seconds=self.claim_duration),
                )
                .order_by("created_at")
            )

            for event_entry in event_entry_iter:
                yield EventPayload.from_event_body(event_entry.body)
                event_entry.claimed_at = util.tz_aware_utcnow()
                await event_entry.save()

    async def is_event_entry_closed(self, event_id: uuid.UUID) -> bool:
        event = await ScheduledEventEntryRelation.filter(event_id=event_id).get()
        return event.closed

    async def close_event_entry(self, event_id: uuid.UUID) -> None:
        event = await ScheduledEventEntryRelation.filter(event_id=event_id).get()
        event.closed = True
        await event.save()
