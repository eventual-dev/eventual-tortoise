import datetime as dt
import uuid
from typing import AsyncContextManager, Iterable, Optional

from eventual import util
from eventual.concurrent.event_send_store import (
    ConcurrentEventSendStore,
    enqueue_after_delay,
)
from eventual.event_store import EventBody, EventReceiveStore, EventStore, Guarantee

from .relation import DispatchedEventRelation, EventOutRelation, HandledEventRelation
from .work_unit import TortoiseWorkUnit


class TortoiseEventStore(EventStore[TortoiseWorkUnit]):
    def create_work_unit(self) -> AsyncContextManager[TortoiseWorkUnit]:
        return TortoiseWorkUnit.create()


class TortoiseEventSendStore(
    TortoiseEventStore, ConcurrentEventSendStore[TortoiseWorkUnit]
):
    async def _write_event_being_scheduled(
        self, event_body: EventBody, send_after: Optional[dt.datetime] = None
    ) -> None:
        event_id = event_body["id"]
        await EventOutRelation.create(
            event_id=event_id,
            body=event_body,
            scheduled_at=util.tz_aware_utcnow(),
            send_after=send_after,
        )

    async def schedule_every_written_event_to_be_sent(self) -> None:
        utc_now = util.tz_aware_utcnow()

        async with TortoiseWorkUnit.create() as work_unit:
            event_obj_seq: Iterable[EventOutRelation] = (
                await EventOutRelation.select_for_update(skip_locked=True)
                .filter(
                    confirmed_sent=False,
                    send_after__lt=utc_now,
                    scheduled_at__lt=utc_now - dt.timedelta(seconds=5),
                )
                .order_by("created_at")
            )

            for event_obj in event_obj_seq:
                # Events are selected in such a way that delay can not be positive.
                delay = 0.0
                self.task_group.start_soon(
                    enqueue_after_delay,
                    self.event_body_send_stream.clone(),
                    event_obj.body,
                    delay,
                )
                event_obj.scheduled_at = util.tz_aware_utcnow()
                await event_obj.save()
            await work_unit.commit()

    async def _mark_event_as_sent(self, event_body: EventBody) -> None:
        event = await EventOutRelation.filter(event_id=event_body["id"]).get()
        event.confirmed_sent = True
        await event.save()


class TortoiseEventReceiveStore(
    TortoiseEventStore, EventReceiveStore[TortoiseWorkUnit]
):
    async def is_event_handled(self, event_id: uuid.UUID) -> bool:
        event_count = await HandledEventRelation.filter(id=event_id).count()
        return event_count > 0

    async def mark_event_as_handled(
        self, event_body: EventBody, guarantee: Guarantee
    ) -> uuid.UUID:
        event_id = event_body["id"]
        await HandledEventRelation.create(
            id=event_id, body=event_body, guarantee=guarantee
        )
        return event_id

    async def mark_event_as_dispatched(self, event_body: EventBody) -> uuid.UUID:
        event_id = event_body["id"]
        await DispatchedEventRelation.create(body=event_body, event_id=event_id)
        return event_id
