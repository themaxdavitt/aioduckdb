# Copyright 2022 Amethyst Reese
# Licensed under the MIT license

import duckdb
from typing import Any, AsyncIterator, Iterable, Optional, Tuple, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from .core import Connection
    import pandas
    import pyarrow


class Cursor:
    # duckdb cursors are the same as duckdb connections
    def __init__(self, conn: "Connection", cursor: duckdb.DuckDBPyConnection) -> None:
        self.iter_chunk_size = conn._iter_chunk_size
        self._conn = conn
        self._cursor = cursor
        self.arraysize = 1

    def __aiter__(self) -> AsyncIterator[Tuple]:
        """The cursor proxy is also an async iterator."""
        return self._fetch_chunked()

    async def _fetch_chunked(self):
        while True:
            rows = await self.fetchmany(self.iter_chunk_size)
            if not rows:
                return
            for row in rows:
                yield row

    async def _execute(self, fn, *args, **kwargs):
        """Execute the given function on the shared connection's thread."""
        r = await self._conn._execute(fn, *args, **kwargs)
        return r

    async def execute(self, sql: str, parameters: Iterable[Any] = None) -> "Cursor":
        """Execute the given query."""
        if parameters is None:
            parameters = []
        await self._execute(self._cursor.execute, sql, parameters)
        return self

    async def executemany(
        self, sql: str, parameters: Iterable[Iterable[Any]]
    ) -> "Cursor":
        """Execute the given multiquery."""
        await self._execute(self._cursor.executemany, sql, parameters)
        return self

    # executescript is as of now not a thing in duckdb
    # async def executescript(self, sql_script: str) -> "Cursor":
    #     """Execute a user script."""
    #     await self._execute(self._cursor.executescript, sql_script)
    #     return self

    async def fetchone(self) -> Optional[Tuple]:
        """Fetch a single row."""
        return await self._execute(self._cursor.fetchone)

    async def fetchmany(self, size: int = None) -> Iterable[Tuple]:
        """Fetch up to `cursor.arraysize` number of rows."""
        args: Tuple[int, ...] = ()
        if size is not None:
            args = (size,)
        else:
            args = (self.arraysize,)
        return await self._execute(self._cursor.fetchmany, *args)

    async def fetchall(self) -> Iterable[Tuple]:
        """Fetch all remaining rows."""
        return await self._execute(self._cursor.fetchall)

    async def fetch_record_batch(self, chunk_size: int = 1000000) -> "pyarrow.lib.RecordBatchReader":
        return await self._execute(self._cursor.fetch_record_batch, chunk_size=chunk_size)

    async def arrow(self) -> "pyarrow.lib.Table":
        return await self._execute(self._cursor.arrow)

    async def df(self) -> "pandas.DataFrame":
        return await self._execute(self._cursor.df)
    
    async def close(self) -> None:
        """Close the cursor."""
        await self._execute(self._cursor.close)

    # rowcount is not currently supported but has already been reported as a bug in the duckdb discord
    # and accepted as such by the developers. probably will be implemented soon but this method is commented
    # due to mypy checks failing
    # @property
    # def rowcount(self) -> int:
    #     return self._cursor.rowcount

    # lastrowid is not supported in duckdb, insert statements with returning are recommended instead

    @property
    def description(self) -> Union[Tuple[Tuple], object]:
        return self._cursor.description

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._cursor

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
