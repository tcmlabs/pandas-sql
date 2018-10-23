from enum import Enum, auto
from typing import Callable, Dict, List, NamedTuple, Optional, Type, TypeVar

import pandas as pd
from toolz.functoolz import identity, pipe

DataFrame = Type[pd.DataFrame]
SourceNames = Dict[str, str]
MappingNames = Dict[str, str]
Sources = Dict[str, DataFrame]
SourceGroupNames = Dict[str, SourceNames]
SourceGroup = Dict[str, Sources]


A = TypeVar("A")
B = TypeVar("B")
T = TypeVar("T")

ColumnNameAlias = Dict[str, str]
Endo = Callable[[A], A]
LeftJoins = List[str]
Mapping = Type[pd.DataFrame]
RightJoins = List[str]
Select = List[ColumnNameAlias]
Table = Type[pd.DataFrame]

Join = NamedTuple(
    "Join",
    [("mapping", Mapping), ("left_joins", LeftJoins), ("right_joins", RightJoins)],
)


class Sql(Enum):
    FROM = auto()
    SELECT = auto()
    JOIN_ON = auto()
    SUB_QUERY = auto()


def map_dict(f: Callable[[A], B]) -> Callable[[Dict[T, A]], Dict[T, B]]:
    def do(dict: Dict[T, A]) -> Dict[T, B]:
        return {k: f(v) for k, v in dict.items()}

    return do


def prefix_column_id(prefix: str, column_name: str) -> Endo[Table]:
    def do(table):
        return table.assign(**{"ID": prefix + table[column_name].astype(str)})

    return do


def merge_table_with_mapping(
    mapping: Mapping, left_joins: LeftJoins, right_joins: RightJoins
) -> Endo[Table]:
    def do(table):
        return pd.merge(
            mapping, table, how="right", left_on=left_joins, right_on=right_joins
        )

    return do


def join_tables(join_on: List[Join]) -> Endo[Table]:
    def do(table):
        return pipe(
            table, *map(lambda merge: merge_table_with_mapping(*merge), join_on)
        )

    return do


def select_columns(aliases: List[ColumnNameAlias]) -> Endo[Table]:
    columns = {source: target for alias in aliases for source, target in alias.items()}

    def do(table):
        return table.rename(columns=columns)[list(columns.values())]

    return do


def sql_query(
    add_id_column: Endo[Table],
    sub_query: Endo[Table],
    join_on: List[Join],
    select: Select,
) -> Endo[Table]:
    def do(table: Table):
        return pipe(
            table,
            add_id_column,
            sub_query,
            join_tables(join_on),
            select_columns(select),
        )

    return do


def concat_map(mapper: Callable):
    def do(dataframes: List[DataFrame]) -> DataFrame:
        return pd.concat(map(mapper, dataframes))

    return do


def field(source: str, target: Optional[str] = None) -> Dict[str, str]:
    return {source: target or source}


def execute_query(inputs, query):
    source = inputs[query[Sql.FROM]]

    return sql_query(
        add_id_column=source["id_column"],
        sub_query=query.get(Sql.SUB_QUERY, identity),
        join_on=query.get(Sql.JOIN_ON, []),
        select=query[Sql.SELECT],
    )(source["data_frame"])


def execute_all(inputs, queries):
    return map_dict(concat_map(execute_query))(queries)


# Usage

users = pd.DataFrame(
    [
        {"id": 1, "first_name": "Alice", "age": 30},
        {"id": 2, "first_name": "Bob", "age": 24},
        {"id": 3, "first_name": "Caroll", "age": 30},
        {"id": 4, "first_name": "Denis", "age": 24},
    ]
)

companies = pd.DataFrame(
    [
        {"id": 1, "name": "ACME Corp", "managerId": 1},
        {"id": 2, "name": "X.Y.Z. Ltd", "managerId": 3},
    ]
)


inputs = {
    "users": {"data_frame": users, "id_column": identity},
    "companies": {"data_frame": pd.DataFrame(), "id_column": identity},
}

query = [
    {
        Sql.SELECT: [field("id"), field("name")],
        Sql.FROM: "users",
        Sql.JOIN_ON: [(inputs["companies"], ["id"], ["managerId"])],
    }
]


print(execute_query(queries))
