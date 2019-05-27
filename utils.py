## -*- coding: UTF-8 -*-
## utils.py
##
## Copyright (c) 2019 analyzeDFIR
##
## Permission is hereby granted, free of charge, to any person obtaining a copy
## of this software and associated documentation files (the "Software"), to deal
## in the Software without restriction, including without limitation the rights
## to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
## copies of the Software, and to permit persons to whom the Software is
## furnished to do so, subject to the following conditions:
##
## The above copyright notice and this permission notice shall be included in all
## copies or substantial portions of the Software.
##
## THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
## IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
## FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
## AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
## LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
## OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
## SOFTWARE.

#pylint: disable=W0613,E0102,R0901
from typing import Any

from sqlalchemy.types import String, Text, NVARCHAR
from sqlalchemy.schema import Table, Column, MetaData, DDLElement
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.engine.interfaces import Compiled
from sqlalchemy.sql.expression import ClauseElement, FromClause
from sqlalchemy.event import listen

def DialectSpecificText() -> String:    #pylint: disable=C0103
    """
    Args:
        N/A
    Returns:
        Text-like SQLAlchemy column dialect abstraction.  Types returned are:
            1) PostgreSQL -> Text
            2) MSSQL -> NVARCHAR(None)
            3) MySQL -> NVARCHAR(None)
            4) Default -> String
    Preconditions:
        N/A
    """
    return String()\
            .with_variant(Text, 'postgresql')\
            .with_variant(NVARCHAR(None), 'mssql')\
            .with_variant(NVARCHAR(None), 'mysql')


class TimestampDefaultExpression(ClauseElement):
    """'Default timestamp expression dialect abstraction. Supported RDBMSs are:
        1) MSSQL
        2) MySQL
        3) Oracle
        4) PostgreSQL
        5) SQLite
    """


@compiles(TimestampDefaultExpression, 'mssql')
def generate_timestamp_expression(
    element: TimestampDefaultExpression,
    compiler: Compiled,
    **kwargs: Any
) -> str:
    return 'GETUTCDATE()'

@compiles(TimestampDefaultExpression, 'mysql')
def generate_timestamp_expression(
    element: TimestampDefaultExpression,
    compiler: Compiled,
    **kwargs
) -> str:
    return 'UTC_TIMESTAMP()'

@compiles(TimestampDefaultExpression, 'oracle')
def generate_timestamp_expression(
    element: TimestampDefaultExpression,
    compiler: Compiled,
    **kwargs
) -> str:
    return 'SYS_EXTRACT_UTC(SYSTIMESTAMP)'

@compiles(TimestampDefaultExpression, 'postgresql')
def generate_timestamp_expression(
    element: TimestampDefaultExpression,
    compiler: Compiled,
    **kwargs
) -> str:
    return '(NOW() AT TIME ZONE \'UTC\')'

@compiles(TimestampDefaultExpression, 'sqlite')
def generate_timestamp_expression(
    element: TimestampDefaultExpression,
    compiler: Compiled,
    **kwargs
) -> str:
    return 'CURRENT_TIMESTAMP'


class CreateViewExpression(DDLElement):
    """Class to allow easy creation of views. Implementation taken from:
    http://www.jeffwidman.com/blog/847/using-sqlalchemy-to-create-and-manage-postgresql-materialized-views/.
    """
    def __init__(self, name: str, selectable: FromClause) -> None:
        self.name = name
        self.selectable = selectable


@compiles(CreateViewExpression)
def generate_view_create_expression(
    element: CreateViewExpression,
    compiler: Compiled,
    **kwargs: Any
) -> str:
    return 'CREATE OR REPLACE VIEW %s AS %s'%(
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True)
    )


class CreateMaterializedViewExpression(CreateViewExpression):
    """Class to allow easy creation of materialized views
    in PostgreSQL (implementation taken from
    http://www.jeffwidman.com/blog/847/using-sqlalchemy-to-create-and-manage-postgresql-materialized-views/)
    """


@compiles(CreateMaterializedViewExpression)
def generate_mview_create_expression(
    element: CreateMaterializedViewExpression,
    compiler: Compiled,
    **kwargs: Any
) -> str:
    return 'CREATE OR REPLACE VIEW %s AS %s'%(
        element.name,
        compiler.process(element.selectable, literal_binds=True)
    )

@compiles(CreateMaterializedViewExpression, 'postgresql')
def generate_mview_create_expression(
    element: CreateMaterializedViewExpression,
    compiler: Compiled,
    **kwargs: Any
) -> str:
    return 'CREATE OR REPLACE MATERIALIZED VIEW %s AS %s'%(
        element.name,
        compiler.process(element.selectable, literal_binds=True)
    )


class DropViewExpression(DDLElement):
    """Class to allow easy deletion of views."""
    def __init__(self, name: str) -> None:
        self.name = name


@compiles(DropViewExpression)
def generate_view_drop_expression(
    element: DropViewExpression,
    compiler: Compiled,
    **kwargs: Any
) -> str:
    return 'DROP VIEW IF EXISTS %s'%(element.name)


class DropMaterializedViewExpression(DropViewExpression):
    """Class to allow easy deletion of materialized views in PostgreSQL."""


@compiles(DropMaterializedViewExpression)
def generate_mview_drop_expression(
    element: DropMaterializedViewExpression,
    compiler: Compiled,
    **kwargs: Any
) -> str:
    return 'DROP VIEW IF EXISTS %s'%(element.name)

@compiles(DropMaterializedViewExpression, 'postgresql')
def generate_mview_drop_expression(
    element: DropMaterializedViewExpression,
    compiler: Compiled,
    **kwargs: Any
) -> str:
    return 'DROP MATERIALIZED VIEW IF EXISTS %s'%(element.name)

def create_view(
    name: str,
    selectable: FromClause,
    metadata: MetaData,
    materialized: bool = False
) -> Table:
    """
    Args:
        name            => name of materialized view to create
        selectable      => query to create view as
        metadata        => metadata to listen for events on
        materialized    => whether to create standard or materialized view
    Returns:
        Table object bound to temporary MetaData object with columns as
        columns returned from selectable (essentially creates table as view).
        NOTE:
            For non-postgresql backends, creating a materialized view
            will result in a standard view, which cannot be indexed.
    Preconditions:
        N/A
    """
    _tmp_mt = MetaData()
    tbl = Table(name, _tmp_mt)
    for column in selectable.c:
        tbl.append_column(
            Column(column.name, column.type, primary_key=column.primary_key)
        )
    listen(
        metadata,
        'after_create',
        (CreateMaterializedViewExpression(name, selectable) \
        if materialized else CreateViewExpression(name, selectable))
    )
    listen(
        metadata,
        'before_drop',
        DropMaterializedViewExpression(name) if materialized else DropViewExpression(name)
    )
    return tbl
