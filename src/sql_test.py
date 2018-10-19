import functools
from typing import List

import pandas as pd
from toolz import compose

import sql

users = pd.DataFrame(
    [
        {"first_name": "Alice", "age": 30},
        {"first_name": "Bob", "age": 24},
        {"first_name": "Caroll", "age": 30},
        {"first_name": "Denis", "age": 24},
    ]
)


def foo(data_frame):
    return data_frame
    # pass


class Database:
    data_frames = {}

    # commands = []

    @staticmethod
    def of(**data_frames):
        return Database(**data_frames)

    def __init__(self, **data_frames):
        self.commands = []
        self.data_frames = data_frames

    def select(self, *columns: str):

        self.commands.append(lambda data_frame: data_frame[columns])

        return self

    def from_(self, table_name: str):
        print("----------")
        print(table_name, self.data_frames[table_name])
        self.commands.append(lambda x: self.data_frames.get(table_name))

        return self

    def to_dataframe(self):

        print(self.commands)
        return compose(*self.commands)


def test_select():

    expected = pd.DataFrame(
        [
            {"first_name": "Alice"},
            {"first_name": "Bob"},
            {"first_name": "Caroll"},
            {"first_name": "Denis"},
        ]
    )
    db = Database(users=users)

    result = db.from_("users").select("first_name").to_dataframe()

    assert result.equals(expected)
