from contextlib import asynccontextmanager
import datetime as dt
import uuid
from typing import AsyncGenerator, Optional, List

from eventual import util
from eventual.dispatch.abc import (
    EventStore,
    EventBody,
    Guarantee,
)

from .relation import (
    EventOutRelation,
    HandledEventRelation,
    DispatchedEventRelation,
)
from .work_unit import create_work_unit, TortoiseWorkUnit


class RelationalEventStore(EventStore[TortoiseWorkUnit]):
    @asynccontextmanager
    def create_work_unit(self) -> AsyncGenerator[TortoiseWorkUnit, None]:
        return create_work_unit()

    async def _write_event_to_send_soon(
        self, body: EventBody, send_after: Optional[dt.datetime] = None
    ) -> None:
        event_id = body["id"]
        await EventOutRelation.create(
            event_id=event_id, body=body, send_after=send_after
        )

    async def is_event_handled(self, event_id: uuid.UUID) -> bool:
        event_count = await HandledEventRelation.filter(id=event_id).count()
        return event_count > 0

    async def mark_event_handled(
        self, event_body: EventBody, guarantee: Guarantee
    ) -> uuid.UUID:
        event_id = event_body["id"]
        await HandledEventRelation.create(
            id=event_id, body=event_body, guarantee=guarantee
        )
        return event_id

    async def mark_event_dispatched(self, event_body: EventBody) -> uuid.UUID:
        event_id = event_body["id"]
        await DispatchedEventRelation.create(body=event_body, event_id=event_id)
        return event_id

    async def _not_confirmed_event_body_seq(
        self,
    ) -> List[EventBody]:
        event_seq = await EventOutRelation.filter(
            confirmed=False, send_after__lt=util.tz_aware_utcnow()
        ).order_by("created_at")

        return [event.body for event in event_seq]

    async def _confirm_event(self, event_id: uuid.UUID) -> None:
        event = await HandledEventRelation.filter(id=event_id).get()
        event.confirmed = True
        await event.save()
