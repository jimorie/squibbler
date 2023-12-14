from __future__ import annotations

from squibbler import Column
from squibbler import (
    Context,
    DeleteQuery,
    InsertQuery,
    SelectQuery,
    Table,
    UpdateQuery,
)


class SQLiteColumn(Column):
    """Column class for the SQLite dialect."""

    pass


class SQLiteContext(Context):
    """Context class for the SQLite dialect."""

    DIALECT_OPERATORS = {**Context.DIALECT_OPERATORS, "{} <> {}": "{} != {}"}


class SQLiteSelectQuery(SelectQuery):
    """SelectQuery class for the SQLite dialect."""

    context_cls = SQLiteContext
    column_cls = SQLiteColumn


class SQLiteInsertQuery(InsertQuery):
    """InsertQuery class for the SQLite dialect."""

    context_cls = SQLiteContext
    column_cls = SQLiteColumn


class SQLiteUpdateQuery(UpdateQuery):
    """UpdateQuery class for the SQLite dialect."""

    context_cls = SQLiteContext
    column_cls = SQLiteColumn


class SQLiteDeleteQuery(DeleteQuery):
    """DeleteQuery class for the SQLite dialect."""

    context_cls = SQLiteContext
    column_cls = SQLiteColumn


class SQLiteTable(Table):
    """
    Table class for the SQLite dialect.

    >>> table = SQLiteTable('mytable')
    >>> table.select().where(table.foo != 42).compile()
    ('SELECT * FROM mytable WHERE mytable.foo != :1', {'1': 42})
    """

    select_cls = SQLiteSelectQuery
    insert_cls = SQLiteInsertQuery
    update_cls = SQLiteUpdateQuery
    delete_cls = SQLiteDeleteQuery
