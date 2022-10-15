import duckdb
import os
import sys
from pathlib import Path
try:
    import pyarrow
    import pyarrow.parquet
    import pyarrow.dataset
    import numpy as np
    can_run = True
except:
    print("CAN'T RUN ARROW TEST")
    can_run = False

if sys.version_info < (3, 8):
    from aiounittest import AsyncTestCase as TestCase
else:
    from unittest import IsolatedAsyncioTestCase as TestCase

import aioduckdb
from .helpers import setup_logger

TEST_DB = Path("test.db")

class TestArrowDataset(TestCase):
    @classmethod
    def setUpClass(cls):
        setup_logger()

    def setUp(self):
        if TEST_DB.exists():
            TEST_DB.unlink()

    def tearDown(self):
        if TEST_DB.exists():
            TEST_DB.unlink()

    async def test_parallel_dataset(self):
        if not can_run:
            return
        async with aioduckdb.connect(TEST_DB) as duckdb_conn:
            await duckdb_conn.execute("PRAGMA threads=4")
            await duckdb_conn.execute("PRAGMA verify_parallelism")

            parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','userdata1.parquet')

            userdata_parquet_dataset= pyarrow.dataset.dataset([
                parquet_filename,
                parquet_filename,
                parquet_filename,
            ] , format="parquet")

            rel = await duckdb_conn.from_arrow(userdata_parquet_dataset)

            self.assertEqual((await
                             (await
                              (await
                               (await rel
                                .filter("first_name=\'Jose\' and salary > 134708.82")
                               ).aggregate('count(*)')
                              ).execute()
                             ).fetchone())[0], 12)

    async def test_parallel_dataset_register(self):
        if not can_run:
            return
        async with aioduckdb.connect(TEST_DB) as duckdb_conn:
            await duckdb_conn.execute("PRAGMA threads=4")
            await duckdb_conn.execute("PRAGMA verify_parallelism")

            parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','userdata1.parquet')

            userdata_parquet_dataset= pyarrow.dataset.dataset([
                parquet_filename,
                parquet_filename,
                parquet_filename,
            ], format="parquet")

            rel = await duckdb_conn.register("dataset",userdata_parquet_dataset)

            self.assertEqual((await (await duckdb_conn.execute_on_self("Select count(*) from dataset where first_name = 'Jose' and salary > 134708.82")).fetchone())[0], 12)

    async def test_parallel_dataset_roundtrip(self):
        if not can_run:
            return

        async with aioduckdb.connect(TEST_DB) as duckdb_conn:
            await duckdb_conn.execute("PRAGMA threads=4")
            await duckdb_conn.execute("PRAGMA verify_parallelism")

            parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','userdata1.parquet')

            userdata_parquet_dataset= pyarrow.dataset.dataset([
                parquet_filename,
                parquet_filename,
                parquet_filename,
            ], format="parquet")

            rel = await duckdb_conn.register("dataset",userdata_parquet_dataset)

            query = await duckdb_conn.execute_on_self("SELECT * FROM dataset order by id" )
            record_batch_reader = await query.fetch_record_batch(2048)

            arrow_table = record_batch_reader.read_all()
            # reorder since order of rows isn't deterministic
            df = userdata_parquet_dataset.to_table().to_pandas().sort_values('id').reset_index(drop=True)
            # turn it into an arrow table
            arrow_table_2 = pyarrow.Table.from_pandas(df)

            self.assertEqual(arrow_table, arrow_table_2)
