# Copyright 2022 Amethyst Reese
# Licensed under the MIT license
import asyncio
import sqlite3
import sys
from pathlib import Path
from duckdb import OperationalError
from threading import Thread
from unittest import skipIf, SkipTest, skipUnless
import pandas

if sys.version_info < (3, 8):
    from aiounittest import AsyncTestCase as TestCase
else:
    from unittest import IsolatedAsyncioTestCase as TestCase

import aioduckdb
from .helpers import setup_logger

TEST_DB = Path("test.db")


class SmokeTest(TestCase):
    @classmethod
    def setUpClass(cls):
        setup_logger()

    def setUp(self):
        if TEST_DB.exists():
            TEST_DB.unlink()

    def tearDown(self):
        if TEST_DB.exists():
            TEST_DB.unlink()

    async def test_connection_await(self):
        db = await aioduckdb.connect(TEST_DB)
        self.assertIsInstance(db, aioduckdb.Connection)

        async with db.execute("select 1, 2") as cursor:
            rows = await cursor.fetchall()
            self.assertEqual(rows, [(1, 2)])

        await db.close()

    async def test_connection_context(self):
        async with aioduckdb.connect(TEST_DB) as db:
            self.assertIsInstance(db, aioduckdb.Connection)

            async with db.execute("select 1, 2") as cursor:
                rows = await cursor.fetchall()
                self.assertEqual(rows, [(1, 2)])

    async def test_connection_locations(self):
        class Fake:  # pylint: disable=too-few-public-methods
            def __str__(self):
                return str(TEST_DB)

        locs = ("test.db", b"test.db", Path("test.db"), Fake())

        async with aioduckdb.connect(TEST_DB) as db:
            await db.execute("create table foo (i integer, k integer)")
            await db.execute("insert into foo (i, k) values (1, 5)")
            await db.commit()

            cursor = await db.execute("select * from foo")
            rows = await cursor.fetchall()

        for loc in locs:
            async with aioduckdb.connect(loc) as db:
                cursor = await db.execute("select * from foo")
                self.assertEqual(await cursor.fetchall(), rows)

    async def test_multiple_connections(self):
        async with aioduckdb.connect(TEST_DB) as db:
            await db.execute(
                "create table multiple_connections "
                "(i integer, k integer)"
            )

            async def do_one_conn(i):
                async with db.cursor() as dbcur:
                    await dbcur.execute("insert into multiple_connections (k) values (?)", [i])
                    # await db.commit()

            await asyncio.gather(*[do_one_conn(i) for i in range(10)])

        async with aioduckdb.connect(TEST_DB) as db:
            cursor = await db.execute("select * from multiple_connections")
            rows = await cursor.fetchall()

        self.assertEqual(len(rows), 10)

    async def test_multiple_queries(self):
        async with aioduckdb.connect(TEST_DB) as db:
            await db.execute(
                "create table multiple_queries "
                "(i integer, k integer)"
            )

            await asyncio.gather(
                *[
                    db.execute("insert into multiple_queries (k) values (?)", [i])
                    for i in range(10)
                ]
            )

            await db.commit()

        async with aioduckdb.connect(TEST_DB) as db:
            cursor = await db.execute("select * from multiple_queries")
            rows = await cursor.fetchall()

        assert len(rows) == 10

    async def test_iterable_cursor(self):
        async with aioduckdb.connect(TEST_DB) as db:
            cursor = await db.cursor()
            await cursor.execute(
                "create table iterable_cursor " "(i integer, k integer)"
            )
            await cursor.executemany(
                "insert into iterable_cursor (k) values (?)", [[i] for i in range(10)]
            )
            await db.commit()

        async with aioduckdb.connect(TEST_DB) as db:
            cursor = await db.execute("select * from iterable_cursor")
            rows = []
            async for row in cursor:
                rows.append(row)

        assert len(rows) == 10

    async def test_multi_loop_usage(self):
        results = {}

        def runner(k, conn):
            async def query():
                async with conn.execute("select * from foo") as cursor:
                    rows = await cursor.fetchall()
                    self.assertEqual(len(rows), 2)
                    return rows

            with self.subTest(k):
                loop = asyncio.new_event_loop()
                rows = loop.run_until_complete(query())
                loop.close()
                results[k] = rows

        async with aioduckdb.connect(":memory:") as db:
            await db.execute("create table foo (id int, name varchar)")
            await db.execute(
                "insert into foo values (?, ?), (?, ?)", (1, "Sally", 2, "Janet")
            )
            await db.commit()

            threads = [Thread(target=runner, args=(k, db)) for k in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        self.assertEqual(len(results), 4)
        for rows in results.values():
            self.assertEqual(len(rows), 2)

    async def test_context_cursor(self):
        async with aioduckdb.connect(TEST_DB) as db:
            async with db.cursor() as cursor:
                await cursor.execute(
                    "create table context_cursor "
                    "(i integer, k integer)"
                )
                await cursor.executemany(
                    "insert into context_cursor (k) values (?)",
                    [[i] for i in range(10)],
                )
                await db.commit()

        async with aioduckdb.connect(TEST_DB) as db:
            async with db.execute("select * from context_cursor") as cursor:
                rows = []
                async for row in cursor:
                    rows.append(row)

        assert len(rows) == 10

    async def test_cursor_return_self(self):
        async with aioduckdb.connect(TEST_DB) as db:
            cursor = await db.cursor()

            result = await cursor.execute(
                "create table test_cursor_return_self (i integer, k integer)"
            )
            self.assertEqual(result, cursor, "cursor execute returns itself")

            result = await cursor.executemany(
                "insert into test_cursor_return_self values (?, ?)", [(1, 1), (2, 2)]
            )
            self.assertEqual(result, cursor)

    async def test_cursor_df(self):
        async with aioduckdb.connect(TEST_DB) as db:
            cursor = await db.cursor()

            await cursor.execute(
                "create table test_cursor_df (i integer, k integer)"
            )

            await cursor.executemany(
                "insert into test_cursor_df values (?, ?)", [(1, 1), (2, 2)]
            )

            cursor = await cursor.execute('select * from test_cursor_df')

            df = await cursor.df()

            self.assertTrue(all(df == pandas.DataFrame([
                {'i':1,'k':2},
                {'i':2,'k':2}
            ])))

    async def test_relation_minmax(self):
        async with aioduckdb.connect(TEST_DB) as db:
            async with db.cursor() as cursor:
                await cursor.execute(
                    "create table test_relation_minmax (i integer, k integer)"
                )

                await cursor.executemany(
                    "insert into test_relation_minmax values (?, ?)", [(1, 7), (2, 2), (3,5)]
                )

            relation = await db.query('select * from test_relation_minmax')

            mini, mink = await (await relation.min('i,k')).fetchone()
            self.assertEqual((mini,mink), (1,2))

            maxi, maxk = await (await relation.max('i,k')).fetchone()
            self.assertEqual((maxi,maxk), (3,7))

    async def test_connection_properties(self):
        async with aioduckdb.connect(TEST_DB) as db:
            # self.assertEqual(db.total_changes, 0)
            # total_changes not available in duckdb

            async with db.cursor() as cursor:
                # self.assertFalse(db.in_transaction)
                # in_transaction not available in duckdb
                await cursor.execute(
                    "create table test_properties "
                    "(i integer, k integer, d text)"
                )
                await cursor.execute(
                    "insert into test_properties (k, d) values (1, 'hi')"
                )
                # self.assertTrue(db.in_transaction)
                # await db.commit()
                # self.assertFalse(db.in_transaction)

            # self.assertEqual(db.total_changes, 1)

            # self.assertIsNone(db.row_factory)
            # self.assertEqual(db.text_factory, default_text_factory)

            async with db.cursor() as cursor:
                await cursor.execute("select * from test_properties")
                row = await cursor.fetchone()
                self.assertIsInstance(row, tuple)
                self.assertEqual(row, (None, 1, "hi"))
                with self.assertRaises(TypeError):
                    _ = row["k"]

            # db.row_factory = aiosqlite.Row
            # db.text_factory = bytes
            # self.assertEqual(db.row_factory, aiosqlite.Row)
            # self.assertEqual(db.text_factory, bytes)
            # row factory not supported in duckdb

            # async with db.cursor() as cursor:
            #     await cursor.execute("select * from test_properties")
            #     row = await cursor.fetchone()
            #     self.assertIsInstance(row, aiosqlite.Row)
            #     self.assertEqual(row[1], 1)
            #     self.assertEqual(row[2], b"hi")
            #     self.assertEqual(row["k"], 1)
            #     self.assertEqual(row["d"], b"hi")

    async def test_fetch_all(self):
        async with aioduckdb.connect(TEST_DB) as db:
            await db.execute(
                "create table test_fetch_all (i integer, k integer)"
            )
            await db.execute(
                "insert into test_fetch_all (k) values (10), (24), (16), (32)"
            )
            # await db.commit()

        async with aioduckdb.connect(TEST_DB) as db:
            cursor = await db.execute("select k from test_fetch_all where k < 30")
            rows = await cursor.fetchall()
            self.assertEqual(rows, [(10,), (24,), (16,)])

    async def test_connect_error(self):
        bad_db = Path("/something/that/shouldnt/exist.db")
        with self.assertRaisesRegex(OperationalError, 'IO Error: Cannot open file "/something/that/shouldnt/exist.db": No such file or directory'):
            async with aioduckdb.connect(bad_db) as db:
                self.assertIsNone(db)  # should never be reached

        with self.assertRaisesRegex(OperationalError, 'IO Error: Cannot open file "/something/that/shouldnt/exist.db": No such file or directory'):
            db = await aioduckdb.connect(bad_db)
            self.assertIsNone(db)  # should never be reached

    async def test_cursor_on_closed_connection(self):
        db = await aioduckdb.connect(TEST_DB)

        cursor = await db.execute("select 1, 2")
        await db.close()
        with self.assertRaisesRegex(ValueError, "Connection closed"):
            await cursor.fetchall()
        with self.assertRaisesRegex(ValueError, "Connection closed"):
            await cursor.fetchall()

    async def test_cursor_on_closed_connection_loop(self):
        db = await aioduckdb.connect(TEST_DB)

        cursor = await db.execute("select 1, 2")
        tasks = []
        for i in range(100):
            if i == 50:
                tasks.append(asyncio.ensure_future(db.close()))
            tasks.append(asyncio.ensure_future(cursor.fetchall()))
        for task in tasks:
            try:
                await task
            except (aioduckdb.ProgrammingError, aioduckdb.InvalidInputException):
                pass
