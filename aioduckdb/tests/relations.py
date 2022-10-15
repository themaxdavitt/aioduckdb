import os
import sys
from pathlib import Path
import tempfile

import numpy as np
import pandas as pd

if sys.version_info < (3, 8):
    from aiounittest import AsyncTestCase as TestCase
else:
    from unittest import IsolatedAsyncioTestCase as TestCase

import aioduckdb
from .helpers import setup_logger

TEST_DB = Path("test.db")

async def get_relation(conn):
    test_df = pd.DataFrame.from_dict({"i":[1, 2, 3, 4], "j":["one", "two", "three", "four"]})
    return await conn.from_df(test_df)

class RelationTest(TestCase):
    @classmethod
    def setUpClass(cls):
        setup_logger()

    def setUp(self):
        if TEST_DB.exists():
            TEST_DB.unlink()

    def tearDown(self):
        if TEST_DB.exists():
            TEST_DB.unlink()

    async def test_filter_operator(self): 
        async with aioduckdb.connect(TEST_DB) as conn:
            rel = await get_relation(conn)
            rel = await rel.filter('i > 1')
            rel = await rel.fetchall()
            self.assertEqual(rel, [(2, 'two'), (3, 'three'), (4, 'four')])

    async def test_projection_operator(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            rel = await get_relation(conn)
            rel = await rel.project('i')
            rel = await rel.fetchall()
            self.assertEqual(rel, [(1,), (2,), (3,), (4,)])

    async def test_projection_operator(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            rel = await get_relation(conn)
            rel = await rel.order('j')
            rel = await rel.fetchall()
            self.assertEqual(rel, [(4, 'four'), (1, 'one'), (3, 'three'), (2, 'two')])

    async def test_limit_operator(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            rel = await get_relation(conn)
            rel = await rel.limit(2)
            rel = await rel.fetchall()
            self.assertEqual(rel, [(1, 'one'), (2, 'two')])

            rel = await get_relation(conn)
            rel = await rel.limit(2,offset=1)
            rel = await rel.fetchall()
            self.assertEqual(rel, [(2, 'two'), (3, 'three')])

    async def test_intersect_operator(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            test_df = pd.DataFrame.from_dict({"i":[1, 2, 3, 4]})
            rel = await conn.from_df(test_df)
            test_df_2 = pd.DataFrame.from_dict({"i":[3, 4, 5, 6]})
            rel_2 = await conn.from_df(test_df_2)

            inter = await rel.intersect(rel_2)
            inter = await inter.fetchall()

            self.assertEqual(inter, [(3,), (4,)])

    async def test_aggregate_operator(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            rel = await get_relation(conn)
            rs = await rel.aggregate('sum(i)')
            rs = await rs.fetchall()
            self.assertEqual(rs, [(10,)])

            rjs = await rel.aggregate('j, sum(i)')
            rjs = await rjs.fetchall()
            self.assertEqual(rjs, [('one', 1), ('two', 2), ('three', 3), ('four', 4)])

    async def test_distinct_operator(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            rel = await get_relation(conn)
            rel = await rel.distinct()
            rel = await rel.fetchall()
            self.assertEqual(rel, [(1, 'one'), (2, 'two'), (3, 'three'),(4, 'four')])

    async def test_union_operator(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            rel = await get_relation(conn)
            rel = await rel.union(rel)
            rel = await rel.fetchall()
            self.assertEqual(rel, [(1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')])

    async def test_join_operator(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            test_df = pd.DataFrame.from_dict({"i":[1, 2, 3, 4], "j":["one", "two", "three", "four"]})
            rel = await conn.from_df(test_df)
            rel2 = await conn.from_df(test_df)
            rel = await rel.join(rel2, 'i')
            rel = await rel.fetchall()
            self.assertEqual(rel, [(1, 'one', 'one'), (2, 'two', 'two'), (3, 'three', 'three'), (4, 'four', 'four')])

    async def test_except_operator(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            test_df = pd.DataFrame.from_dict({"i":[1, 2, 3, 4], "j":["one", "two", "three", "four"]})
            rel = await conn.from_df(test_df)
            rel2 = await conn.from_df(test_df)
            exc = await rel.except_(rel2)
            self.assertEqual(await exc.fetchall(), [])

    async def test_create_operator(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            test_df = pd.DataFrame.from_dict({"i":[1, 2, 3, 4], "j":["one", "two", "three", "four"]})
            rel = await conn.from_df(test_df)
            await rel.create("test_df")
            r = await conn.query("select * from test_df")
            self.assertEqual(await r.fetchall(), [(1, 'one'), (2, 'two'), (3, 'three'),(4, 'four')])

    async def test_create_view_operator(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            test_df = pd.DataFrame.from_dict({"i":[1, 2, 3, 4], "j":["one", "two", "three", "four"]})
            rel = await conn.from_df(test_df)
            await rel.create_view("test_df")
            r = await conn.query("select * from test_df")
            self.assertEqual(await r.fetchall(), [(1, 'one'), (2, 'two'), (3, 'three'),(4, 'four')])

    async def test_insert_into_operator(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            rel = await get_relation(conn)
            await rel.create("test_table2")
            # insert the relation's data into an existing table
            await conn.execute("CREATE TABLE test_table3 (i INTEGER, j STRING)")
            await rel.insert_into("test_table3")

            # Inserting elements into table_3
            await (await conn.values([5, 'five'])).insert_into("test_table3")
            rel_3 = await conn.table("test_table3")
            await rel_3.insert([6,'six'])

            self.assertEqual(await rel_3.fetchall(), [(1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five'), (6, 'six')])
        
    async def test_write_csv_operator(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            df_rel = await get_relation(conn)
            temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
            await df_rel.write_csv(temp_file_name)

            csv_rel = await conn.from_csv_auto(temp_file_name)
            self.assertEqual(await df_rel.fetchall(), await csv_rel.fetchall())

    async def test_get_attr_operator(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            await conn.execute("CREATE TABLE test (i INTEGER)")
            rel = await conn.table("test")
            assert rel.alias == "test"
            assert rel.type == "TABLE_RELATION"
            assert rel.columns == ['i']
            assert rel.types == ['INTEGER']

    async def test_query_fail(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            await conn.execute("CREATE TABLE test (i INTEGER)")
            rel = await conn.table("test")
            with self.assertRaisesRegex(TypeError, 'incompatible function arguments'):
                await rel.query("select j from test")

    async def test_execute_fail(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            await conn.execute("CREATE TABLE test (i INTEGER)")
            rel = await conn.table("test")
            with self.assertRaisesRegex(TypeError, 'incompatible function arguments'):
                await rel.execute("select j from test")

    # def test_df_proj(self):
    #     test_df = pd.DataFrame.from_dict({"i":[1, 2, 3, 4], "j":["one", "two", "three", "four"]})
    #     rel = duckdb.project(test_df, 'i')
    #     assert rel.execute().fetchall() == [(1,), (2,), (3,), (4,)]

    # def test_df_alias(self):
    #     test_df = pd.DataFrame.from_dict({"i":[1, 2, 3, 4], "j":["one", "two", "three", "four"]})
    #     rel = duckdb.alias(test_df, 'dfzinho')
    #     assert rel.alias == "dfzinho"

    # def test_df_filter(self):
    #     test_df = pd.DataFrame.from_dict({"i":[1, 2, 3, 4], "j":["one", "two", "three", "four"]})
    #     rel = duckdb.filter(test_df, 'i > 1')
    #     assert rel.execute().fetchall() == [(2, 'two'), (3, 'three'), (4, 'four')]

    # def test_df_order_by(self):
    #     test_df = pd.DataFrame.from_dict({"i":[1, 2, 3, 4], "j":["one", "two", "three", "four"]})
    #     rel = duckdb.order(test_df, 'j')
    #     assert rel.execute().fetchall() == [(4, 'four'), (1, 'one'), (3, 'three'), (2, 'two')]

    # def test_df_distinct(self):
    #     test_df = pd.DataFrame.from_dict({"i":[1, 2, 3, 4], "j":["one", "two", "three", "four"]})
    #     rel = duckdb.distinct(test_df)
    #     assert rel.execute().fetchall() == [(1, 'one'), (2, 'two'), (3, 'three'),(4, 'four')]

    # def test_df_write_csv(self):
    #     test_df = pd.DataFrame.from_dict({"i":[1, 2, 3, 4], "j":["one", "two", "three", "four"]})
    #     temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
    #     duckdb.write_csv(test_df, temp_file_name)
    #     csv_rel = duckdb.from_csv_auto(temp_file_name)
    #     assert  csv_rel.execute().fetchall() == [(1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')]


    async def test_join_types(self):
        async with aioduckdb.connect(TEST_DB) as conn:
            test_df1 = pd.DataFrame.from_dict({"i":[1, 2, 3, 4]})
            test_df2 = pd.DataFrame.from_dict({"j":[  3, 4, 5, 6]})
            rel1 = await conn.from_df(test_df1)
            rel2 = await conn.from_df(test_df2)

            self.assertEqual((await (await (await rel1.join(rel2, 'i=j', 'inner')).aggregate('count()')).fetchone())[0], 2)
            
            self.assertEqual((await (await (await rel1.join(rel2, 'i=j', 'left')).aggregate('count()')).fetchone())[0], 4)

    # def test_explain(self):
    #     con = duckdb.connect()
    #     con.execute("Create table t1 (i integer)")
    #     con.execute("Create table t2 (j integer)")
    #     rel1 = con.table('t1')
    #     rel2 = con.table('t2')
    #     join = rel1.join(rel2, 'i=j', 'inner').aggregate('count()')
    #     assert join.explain() == 'Aggregate [count_star()]\n  Join INNER (i = j)\n    Scan Table [t1]\n    Scan Table [t2]'

    async def test_fetchnumpy(self):
        start, stop = -1000, 2000
        count = stop - start

        async with aioduckdb.connect(TEST_DB) as con:
            await con.execute(f"CREATE table t AS SELECT range AS a FROM range({start}, {stop});")
            rel = await con.table("t")
        
            # empty
            res = await (await rel.limit(0, offset=count + 1)).fetchnumpy()
            assert set(res.keys()) == {"a"}
            assert len(res["a"]) == 0

            # < vector_size, == vector_size, > vector_size
            for size in [1000, 1024, 1100]:
                res = await (await (await rel.project("a")).limit(size)).fetchnumpy()
                assert set(res.keys()) == {"a"}
                # For some reason, this return a masked array. Shouldn't it be
                # known that there can't be NULLs?
                if isinstance(res, np.ma.MaskedArray):
                    assert res.count() == size
                    res = res.compressed()
                else:
                    assert len(res["a"]) == size
                assert np.all(res["a"] == np.arange(start, start + size))

            with self.assertRaisesRegex(aioduckdb.ConversionException, "Conversion Error.*out of range.*"):
                # invalid conversion of negative integer to UINTEGER
                await (await rel.project("CAST(a as UINTEGER)")).fetchnumpy()

