from functools import reduce
from typing import Callable, Dict, List, Type, TypeVar

import pandas as pd

A = TypeVar("A")
Columns = List[str]
Endo = Callable[[A], A]
Table = Type[pd.DataFrame]
Database = Dict[str, Table]


def query(*functions):
    return reduce(lambda fs, f: f(fs), functions)


class Query:
    def select(self, columns: Columns):
        def inject_db(db: Database):
            def do(table):
                return table[columns]

            return do

        return inject_db

    def from_(self, table_name: str):
        def _chain(chain):
            def inject_db(db: Database):
                def do(table=None):
                    return chain(db)(db[table_name])

                return do

            return inject_db

        return _chain

    def _join_on(self, table_name, left_on, right_on, how):
        def _chain(chain):
            def inject_db(db):
                def do(_table=None):
                    return pd.merge(
                        db[table_name],
                        chain(db)(),
                        how=how,
                        left_on=left_on,
                        right_on=right_on,
                    )

                return do

            return inject_db

        return _chain

    def left_join(self, table_name, left_on, right_on):
        def _chain(chain):
            return self._join_on(table_name, left_on, right_on, how="left")(chain)

        return _chain


users = pd.DataFrame(
    [
        {"first_name": "Alice", "age": 30},
        {"first_name": "Bob", "age": 24},
        {"first_name": "Caroll", "age": 30},
        # {"first_name": "Denis", "age": 24},
    ]
)

users2 = pd.DataFrame(
    [
        {"first_name2": "Alice", "age": 30},
        {"first_name2": "Bob", "age": 24},
        {"first_name2": "Caroll", "age": 30},
        {"first_name2": "Denis", "age": 24},
    ]
)

db = {"users": users, "users2": users2}

sql = Query()

res = query(
    sql.select(["first_name"]),
    sql.from_("users"),
    sql.left_join("users2", "first_name2", "first_name"),
)(db)()

print(res)
