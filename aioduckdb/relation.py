import duckdb
from typing import Any, AsyncIterator, Iterable, Optional, Tuple, TYPE_CHECKING, Union
from .result import Result

if TYPE_CHECKING:
    from .core import Connection
    import pyarrow
    import pandas

class Relation:
    def __init__(self, conn: "Connection", relation: duckdb.DuckDBPyRelation) -> None:
        self.relation = relation
        self._conn = conn

    async def _execute(self, fn, *args, **kwargs):
        """Execute the given function on the shared connection's thread."""
        r = await self._conn._execute(fn, *args, **kwargs)
        return r

    async def abs(self, aggregation_columns: str) -> "Relation":
        rel = await self._execute(self.relation.abs, aggregation_columns)
        return Relation(self._conn, rel)

    async def aggregate(self, aggr_expr: str, group_expr: str = '') -> "Relation":
        rel = await self._execute(self.relation.aggregate, aggr_expr, group_expr=group_expr)
        return Relation(self._conn, rel)

    @property
    def alias(self):
        return self.relation.alias

    async def apply(self, function_name: str, function_aggr: str,
                    group_expr: str = '', function_parameter: str = '', projected_columns: str = '') -> "Relation":
        rel = await self._execute(self.relation.apply, function_name, function_aggr,
                                  group_expr=group_expr, function_parameter=function_parameter,
                                  projected_columns=projected_columns)
        return Relation(self._conn, rel)

    async def arrow(self) -> "pyarrow.lib.Table":
        return await self._execute(self.relation.arrow)

    @property
    def columns(self):
        return self.relation.columns
    
    async def count(self, count_aggr: str, group_expr: str = '') -> "Relation":
        rel = await self._execute(self.relation.count, count_aggr, group_expr=group_expr)
        return Relation(self._conn, rel)

    async def create(self, table_name: str) -> None:
        await self._execute(self.relation.create, table_name)

    async def create_view(self, view_name: str, replace: bool = True) -> "Relation":
        rel = await self._execute(self.relation.create_view, view_name, replace=replace)
        return Relation(self._conn, rel)

    async def cummax(self, aggregation_columns: str) -> "Relation":
        rel = await self._execute(self.relation.cummax, aggregation_columns)
        return Relation(self._conn, rel)

    async def cummin(self, aggregation_columns: str) -> "Relation":
        rel = await self._execute(self.relation.cummin, aggregation_columns)
        return Relation(self._conn, rel)

    async def cumprod(self, aggregation_columns: str) -> "Relation":
        rel = await self._execute(self.relation.cumprod, aggregation_columns)
        return Relation(self._conn, rel)

    async def cumsum(self, aggregation_columns: str) -> "Relation":
        rel = await self._execute(self.relation.cumsum, aggregation_columns)
        return Relation(self._conn, rel)

    async def describe(self) -> "Relation":
        rel = await self._execute(self.relation.describe)
        return Relation(self._conn, rel)

    async def df(self) -> "pandas.DataFrame":
        return await self._execute(self.relation.df)

    async def distinct(self) -> "Relation":
        rel = await self._execute(self.relation.distinct)
        return Relation(self._conn, rel)

    @property
    def dtypes(self):
        return self.relation.dtypes

    async def except_(self, other_rel: "Relation") -> "Relation":
        rel = await self._execute(self.relation.except_, other_rel.relation)
        return Relation(self._conn, rel)

    # relation execute does not have any arguments and none are even used in the c++ source code
    # but the original tests pass it a query. not sure what this is about but just in case any positional
    # arguments are passed through.
    async def execute(self, *args) -> Result:
        return Result(self._conn, await self._execute(self.relation.execute, *args))

    async def explain(self) -> str:
        return await self._execute(self.relation.explain)

    async def fetchall(self) -> Union[Iterable[Tuple], object]:
        return await self._execute(self.relation.fetchall)

    async def fetchmany(self, size=1) -> Union[Iterable[Tuple], object]:
        return await self._execute(self.relation.fetchmany, size)

    async def fetchnumpy(self) -> dict:
        return await self._execute(self.relation.fetchnumpy)

    async def fetchone(self) -> Union[Tuple, object]:
        return await self._execute(self.relation.fetchone)

    async def filter(self, filter_expr: str) -> "Relation":
        rel = await self._execute(self.relation.filter, filter_expr)
        return Relation(self._conn, rel)

    async def insert(self, values: Tuple):
        await self._execute(self.relation.insert, values)

    async def insert_into(self, table_name: str):
        await self._execute(self.relation.insert_into, table_name)

    async def intersect(self, other_rel: "Relation") -> "Relation":
        rel = await self._execute(self.relation.intersect, other_rel.relation)
        return Relation(self._conn, rel)

    async def join(self, other_rel: "Relation", condition: str, how: str = 'inner') -> "Relation":
        rel = await self._execute(self.relation.join, other_rel.relation,
                                  condition=condition,
                                  how=how)
        return Relation(self._conn, rel)

    async def kurt(self, aggregation_columns: str, group_columns: str = '') -> "Relation":
        rel = await self._execute(self.relation.kurt, aggregation_columns, group_columns=group_columns)
        return Relation(self._conn, rel)

    async def limit(self, n: int, offset: int = 0) -> "Relation":
        rel = await self._execute(self.relation.limit, n, offset=offset)
        return Relation(self._conn, rel)

    async def mad(self, aggregation_columns: str, group_columns: str = '') -> "Relation":
        rel = await self._execute(self.relation.mad, aggregation_columns, group_columns=group_columns)
        return Relation(self._conn, rel)

    async def map(self, map_function) -> "Relation":
        rel = await self._execute(self.relation.map, map_function)
        return Relation(self._conn, rel)

    async def max(self, aggr: str, group_expr: str = '') -> "Relation":
        rel = await self._execute(self.relation.max, aggr, group_expr=group_expr)
        return Relation(self._conn, rel)

    async def min(self, aggr: str, group_expr: str = '') -> "Relation":
        rel = await self._execute(self.relation.min, aggr, group_expr=group_expr)
        return Relation(self._conn, rel)

    async def mean(self, aggr: str, group_expr: str = '') -> "Relation":
        rel = await self._execute(self.relation.mean, aggr, group_expr=group_expr)
        return Relation(self._conn, rel)

    async def median(self, aggr: str, group_expr: str = '') -> "Relation":
        rel = await self._execute(self.relation.median, aggr, group_expr=group_expr)
        return Relation(self._conn, rel)

    async def mode(self, aggr: str, group_expr: str = '') -> "Relation":
        rel = await self._execute(self.relation.mode, aggr, group_expr=group_expr)
        return Relation(self._conn, rel)

    async def order(self, order_expr: str) -> "Relation":
        rel = await self._execute(self.relation.order, order_expr)
        return Relation(self._conn, rel)

    async def prod(self, aggregation_columns: str, group_columns: str = '') -> "Relation":
        rel = await self._execute(self.relation.prod, aggregation_columns, group_columns=group_columns)
        return Relation(self._conn, rel)

    async def project(self, project_expr: str) -> "Relation":
        rel = await self._execute(self.relation.project, project_expr)
        return Relation(self._conn, rel)

    async def quantile(self, q: str, quantile_aggr: str, group_expr: str = '') -> "Relation":
        rel = await self._execute(self.relation.quantile, q, quantile_aggr, group_expr=group_expr)
        return Relation(self._conn, rel)

    async def query(self, query: str, alias: str = 'query_relation') -> "Relation":
        rel = await self._execute(self.relation.query, query, alias=alias)
        return Relation(self._conn, rel)

    async def sem(self, aggregation_columns: str, group_columns: str = '') -> "Relation":
        rel = await self._execute(self.relation.sem, aggregation_columns, group_columns=group_columns)
        return Relation(self._conn, rel)

    @property
    def shape(self):
        return self.relation.shape

    async def skew(self, aggregation_columns: str, group_columns: str = '') -> "Relation":
        rel = await self._execute(self.relation.skew, aggregation_columns, group_columns=group_columns)
        return Relation(self._conn, rel)

    async def std(self, aggregation_columns: str, group_expr: str = '') -> "Relation":
        rel = await self._execute(self.relation.std, aggregation_columns, group_expr=group_expr)
        return Relation(self._conn, rel)


    async def sum(self, aggregation_columns: str, group_expr: str = '') -> "Relation":
        rel = await self._execute(self.relation.sum, aggregation_columns, group_expr=group_expr)
        return Relation(self._conn, rel)

    @property
    def type(self):
        return self.relation.type

    @property
    def types(self):
        return self.relation.types

    async def union(self, other_rel: "Relation") -> "Relation":
        rel = await self._execute(self.relation.union, other_rel.relation)
        return Relation(self._conn, rel)

    async def unique(self, unique_aggr: str) -> "Relation":
        rel = await self._execute(self.relation.unique, unique_aggr)
        return Relation(self._conn, rel)

    async def var(self, var_aggr: str, group_expr: str = '') -> "Relation":
        rel = await self._execute(self.relation.var, var_aggr, group_expr=group_expr)
        return Relation(self._conn, rel)

    async def write_csv(self, file_name: str) -> None:
        await self._execute(self.relation.write_csv, file_name)
