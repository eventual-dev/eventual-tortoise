import contextlib
from typing import AsyncGenerator

from eventual.abc.work_unit import InterruptWork, WorkUnit
from tortoise.transactions import in_transaction


class TortoiseWorkUnit(WorkUnit):
    def __init__(self) -> None:
        self._committed = False

    @property
    def committed(self) -> bool:
        return self._committed

    @classmethod
    @contextlib.asynccontextmanager
    async def create(cls) -> AsyncGenerator["TortoiseWorkUnit", None]:
        work_unit = TortoiseWorkUnit()
        try:
            async with in_transaction("default"):
                yield work_unit
                work_unit._committed = True
        except InterruptWork:
            work_unit._committed = False
