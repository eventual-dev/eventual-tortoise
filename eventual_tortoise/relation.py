import datetime as dt
from typing import Any, Dict, cast

import orjson
from eventual.event_store import Guarantee
from tortoise import Model, fields
from tortoise.indexes import Index

from . import mixin


class PkUuidModel(Model):
    id = fields.UUIDField(pk=True)

    class Meta:
        abstract = True


def _dump_str(mapping: Dict[str, Any]) -> str:
    return orjson.dumps(mapping).decode()


class EventOutRelation(Model, mixin.Timestamp):
    event_id = fields.UUIDField()
    body = fields.JSONField(encoder=_dump_str, decoder=orjson.loads)
    send_after = fields.DatetimeField(index=True, null=True, default=None)
    scheduled_at: dt.datetime = cast(dt.datetime, fields.DatetimeField(index=True))
    confirmed_sent: bool = cast(bool, fields.BooleanField(default=False))

    class Meta:
        table = "event_out"
        indexes = [
            Index(fields={"created_at"}),
        ]


class DispatchedEventRelation(Model, mixin.Timestamp):
    event_id = fields.UUIDField()
    body = fields.JSONField(encoder=_dump_str, decoder=orjson.loads)

    class Meta:
        table = "dispatched_event"


class HandledEventRelation(PkUuidModel, mixin.Timestamp):
    body = fields.JSONField(encoder=_dump_str, decoder=orjson.loads)
    guarantee = fields.CharEnumField(Guarantee)

    class Meta:
        table = "handled_event"
