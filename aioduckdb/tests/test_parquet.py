import duckdb
import os
import sys
from pathlib import Path

if sys.version_info < (3, 8):
    from aiounittest import AsyncTestCase as TestCase
else:
    from unittest import IsolatedAsyncioTestCase as TestCase

import aioduckdb
from .helpers import setup_logger

TEST_DB = Path("test.db")

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','binary_string.parquet')

class TestParquet(TestCase):
    @classmethod
    def setUpClass(cls):
        setup_logger()

    def setUp(self):
        if TEST_DB.exists():
            TEST_DB.unlink()

    def tearDown(self):
        if TEST_DB.exists():
            TEST_DB.unlink()

    async def test_scan_binary(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            res = await(await conn.execute_on_self("SELECT typeof(#1) FROM parquet_scan('"+filename+"') limit 1")).fetchall()
            self.assertEqual(res[0], ('BLOB',))

            res = await(await conn.execute_on_self("SELECT * FROM parquet_scan('"+filename+"')")).fetchall()
            self.assertEqual(res[0], (b'foo',))

    async def test_from_parquet_binary(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            rel = await conn.from_parquet(filename)
            self.assertEqual(rel.types, ['BLOB'])

            res = await (await rel.execute()).fetchall()
            self.assertEqual(res[0], (b'foo',))

    async def test_scan_binary_as_string(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            res = await (await conn.execute_on_self("SELECT typeof(#1) FROM parquet_scan('"+filename+"',binary_as_string=True) limit 1")).fetchall()
            self.assertEqual(res[0], ('VARCHAR',))

            res = await (await conn.execute_on_self("SELECT * FROM parquet_scan('"+filename+"',binary_as_string=True)")).fetchall()
            self.assertEqual(res[0], ('foo',))

    async def test_from_parquet_binary_as_string(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            rel = await conn.from_parquet(filename,True)
            self.assertEqual(rel.types, ['VARCHAR'])

            res = await (await rel.execute()).fetchall()
            self.assertEqual(res[0], ('foo',))

    async def test_parquet_binary_as_string_pragma(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            res = await (await conn.execute_on_self("SELECT typeof(#1) FROM parquet_scan('"+filename+"') limit 1")).fetchall()
            self.assertEqual(res[0], ('BLOB',))

            res = await (await conn.execute_on_self("SELECT * FROM parquet_scan('"+filename+"')")).fetchall()
            self.assertEqual(res[0], (b'foo',))

            await conn.execute_on_self("PRAGMA binary_as_string=1")
        
            res = await (await conn.execute_on_self("SELECT typeof(#1) FROM parquet_scan('"+filename+"') limit 1")).fetchall()
            self.assertEqual(res[0], ('VARCHAR',))

            res = await (await conn.execute_on_self("SELECT * FROM parquet_scan('"+filename+"')")).fetchall()
            self.assertEqual(res[0], ('foo',))

            res = await (await conn.execute_on_self("SELECT typeof(#1) FROM parquet_scan('"+filename+"',binary_as_string=False) limit 1")).fetchall()
            self.assertEqual(res[0], ('BLOB',))

            res = await (await conn.execute_on_self("SELECT * FROM parquet_scan('"+filename+"',binary_as_string=False)")).fetchall()
            self.assertEqual(res[0], (b'foo',))

            await conn.execute_on_self("PRAGMA binary_as_string=0")

            res = await (await conn.execute_on_self("SELECT typeof(#1) FROM parquet_scan('"+filename+"') limit 1")).fetchall()
            self.assertEqual(res[0], ('BLOB',))

            res = await (await conn.execute_on_self("SELECT * FROM parquet_scan('"+filename+"')")).fetchall()
            self.assertEqual(res[0], (b'foo',))
