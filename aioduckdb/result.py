import duckdb
from typing import Any, AsyncIterator, Iterable, Optional, Tuple, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from .core import Connection
    import pyarrow
    import pandas

class Result:
    def __init__(self, conn: "Connection", result: duckdb.DuckDBPyResult) -> None:
        self.result = result
        self._conn = conn

    async def _execute(self, fn, *args, **kwargs):
        """Execute the given function on the shared connection's thread."""
        r = await self._conn._execute(fn, *args, **kwargs)
        return r

    async def arrow(self) -> "pyarrow.lib.Table":
        return await self._execute(self.result.arrow)

    async def description(self) -> list:
        return await self._execute(self.result.description)

    async def df(self) -> "pandas.DataFrame":
        return await self._execute(self.result.df)

    async def fetchall(self) -> list:
        return await self._execute(self.result.fetchall)

    async def fetchmany(self, size: int = 1) -> list:
        return await self._execute(self.result.fetchmany, size)

    async def fetchnumpy(self) -> dict:
        return await self._execute(self.result.fetchnumpy)

    async def fetchone(self) -> Union[object, Tuple]:
        return await self._execute(self.result.fetchone)
