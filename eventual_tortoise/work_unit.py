import contextlib
from typing import AsyncGenerator

from eventual.work_unit import InterruptWork, WorkUnit
from tortoise.transactions import in_transaction


class TortoiseWorkUnit(WorkUnit):
    def __init__(self) -> None:
        self._commit = False

    async def commit(self) -> None:
        self._commit = True

    async def rollback(self) -> None:
        raise InterruptWork

    @property
    def committed(self) -> bool:
        return self._commit

    @classmethod
    @contextlib.asynccontextmanager
    async def create(cls) -> AsyncGenerator["TortoiseWorkUnit", None]:
        work_unit = TortoiseWorkUnit()
        try:
            async with in_transaction("default"):
                yield work_unit
                if not work_unit._commit:
                    raise InterruptWork
        except InterruptWork:
            work_unit._commit = False
