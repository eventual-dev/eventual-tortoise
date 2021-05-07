import datetime as dt
import uuid
from typing import AsyncGenerator, Optional

from eventual import util
from eventual.dispatch.abc import (
    EventStore,
    EventBody,
    Guarantee, EventReceiveStore,
)
from eventual.dispatch.concurrent_send_store import ConcurrentEventSendStore

from .relation import (
    EventOutRelation,
    HandledEventRelation,
    DispatchedEventRelation,
)
from .work_unit import create_work_unit, TortoiseWorkUnit


class TortoiseEventStore(EventStore[TortoiseWorkUnit]):
    def create_work_unit(self) -> AsyncGenerator[TortoiseWorkUnit, None]:
        return create_work_unit()


class TortoiseEventSendStore(TortoiseEventStore, ConcurrentEventSendStore[TortoiseWorkUnit]):
    async def _write_event_to_send_soon(
            self, body: EventBody, send_after: Optional[dt.datetime] = None
    ) -> None:
        event_id = body["id"]
        await EventOutRelation.create(
            event_id=event_id, body=body, send_after=send_after
        )

    async def _mark_event_as_sent(self, event_body: EventBody):
        event = await EventOutRelation.filter(event_id=event_body["id"]).get()
        event.confirmed = True
        await event.save()

    async def schedule_every_written_event_to_send(self) -> None:
        event_obj_seq = await EventOutRelation.filter(
            confirmed=False, send_after__lt=util.tz_aware_utcnow()
        ).order_by("created_at")

        for event_obj in event_obj_seq:
            timedelta = event_obj.send_after - util.tz_aware_utcnow()
            delay = timedelta.total_seconds()
            if delay < 0.0:
                delay = 0.0
            self.task_group.start_soon(self.enqueue_to_send_after_delay, event_obj.body, delay)


class TortoiseEventReceiveStore(TortoiseEventStore, EventReceiveStore[TortoiseWorkUnit]):
    async def is_event_handled(self, event_id: uuid.UUID) -> bool:
        event_count = await HandledEventRelation.filter(id=event_id).count()
        return event_count > 0

    async def mark_event_as_handled(self, event_body: EventBody, guarantee: Guarantee) -> uuid.UUID:
        event_id = event_body["id"]
        await HandledEventRelation.create(
            id=event_id, body=event_body, guarantee=guarantee
        )
        return event_id

    async def mark_event_as_dispatched(self, event_body: EventBody) -> uuid.UUID:
        event_id = event_body["id"]
        await DispatchedEventRelation.create(body=event_body, event_id=event_id)
        return event_id
