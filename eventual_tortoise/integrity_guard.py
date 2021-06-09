import uuid
from typing import AsyncContextManager

from eventual.abc.guarantee import Guarantee
from eventual.abc.router import IntegrityGuard
from eventual.model import EventPayload

from .relation import DispatchedEventRelation, HandledEventRelation
from .work_unit import TortoiseWorkUnit


class TortoiseIntegrityGuard(IntegrityGuard[TortoiseWorkUnit]):
    def create_work_unit(self) -> AsyncContextManager[TortoiseWorkUnit]:
        return TortoiseWorkUnit.create()

    async def is_dispatch_forbidden(self, event_id: uuid.UUID) -> bool:
        event_count = await HandledEventRelation.filter(id=event_id).count()
        return event_count > 0

    async def record_completion_with_guarantee(
        self, event_payload: EventPayload, guarantee: Guarantee
    ) -> uuid.UUID:
        event_id = event_payload.id
        await HandledEventRelation.create(
            id=event_id, body=event_payload.body, guarantee=guarantee
        )
        return event_id

    async def record_dispatch_attempt(self, event_payload: EventPayload) -> uuid.UUID:
        event_id = event_payload.id
        await DispatchedEventRelation.create(body=event_payload.body, event_id=event_id)
        return event_id
