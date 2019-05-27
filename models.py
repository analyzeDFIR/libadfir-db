## -*- coding: UTF8 -*-
## models.py
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

from typing import Dict, Any

import re
from sqlalchemy.types import Integer, TIMESTAMP, Boolean
from sqlalchemy.sql.schema import Column, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base, declared_attr

from .utils import TimestampDefaultExpression, DialectSpecificText


class BaseTableTemplate:
    """SQLAlchemy declarative base table template class with convenience method
    for populating an ORM table instance with fields from a dictionary."""
    __KEY_REGEX_01 = re.compile(r'(.)([A-Z][a-z]+)')
    __KEY_REGEX_02 = re.compile(r'([a-z0-9])([A-Z])')

    @classmethod
    def __convert_key(cls, key: str) -> str:
        """
        Args:
            key => key to convert
        Returns:
            Key converted from camel case to snake case.
            NOTE:
                Implementation taken from:
                https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case#1176023
        Preconditions:
            N/A
        """
        return re.sub(
            cls.__KEY_REGEX_02,
            r'\1_\2',
            re.sub(cls.__KEY_REGEX_01, r'\1_\2', key)
        ).lower()

    @declared_attr
    def __tablename__(cls) -> str:  #pylint: disable=E0213
        return cls.__name__.lower() #pylint: disable=E1101

    def populate_fields(self,
        data_dict: Dict[str, Any],
        overwrite: bool = True
    ) -> 'BaseTableTemplate':
        """
        Args:
            data_dict   => dict containing data to map to fields
            overwrite   => whether to overwrite values of current instance
        Procedure:
            Populate attributes of this instance with values from data_dict
            where each key in data_dict maps a value to an attribute.
            For example, to populate id and created_at, data_dict would be:
            {
                'id': <Integer>,
                'created_at': <DateTime>
            }
        Preconditions:
            data_dict is of type Dict<String, Any>
        """
        for key in data_dict:
            converted_key = self.__convert_key(key)
            if hasattr(self, converted_key) and \
               (getattr(self, converted_key) is None or overwrite):
                setattr(self, converted_key, data_dict[key])
        return self


BaseTable = declarative_base(cls=BaseTableTemplate) #pylint: disable=C0103


class TableMixin:
    """Mixin class for (non-view) tables."""
    id = Column(Integer, primary_key=True)
    created_at = Column(
        TIMESTAMP(timezone=True),
        server_default=TimestampDefaultExpression(),
        index=True
    )
    __table_args__ = dict(
        mysql_engine='InnoDB',
        mysql_charset='utf8mb4'
    )


class FileLedgerMixin(TableMixin):
    """Mixin for parsed file ledger (tracking) table, which
    serves as accounting system for parsers.  Designed to work with
    FileMetadataMixin in the libadfir-parsers library.
    """
    file_name = Column(DialectSpecificText(), nullable=False)
    file_path = Column(DialectSpecificText(), nullable=False)
    file_size = Column(Integer, nullable=False)
    md5hash = Column(DialectSpecificText())
    sha1hash = Column(DialectSpecificText())
    sha2hash = Column(DialectSpecificText())
    modify_time = Column(TIMESTAMP(timezone=True))
    access_time = Column(TIMESTAMP(timezone=True))
    create_time = Column(TIMESTAMP(timezone=True))
    completed = Column(Boolean, index=True)


class FileLedgerLinkedMixin:
    """Mixin for tables linked to fileledger table (see: FileLedgerMixin).
    Creates a foreign key called ledger_id that links to the fileledger table,
    and thus assumes the fileledger table exists.
    """
    @declared_attr
    def ledger_id(cls): #pylint: disable=E0213,R0201
        return Column(
            Integer,
            ForeignKey('fileledger.id', ondelete='CASCADE', onupdate='CASCADE'),
            nullable=False,
            index=True
        )


class SharedStructureTableMixin:
    """Mixin for tables storing data present in multiple structures."""
    structure_id = Column(Integer, nullable=False)
    structure_type = Column(DialectSpecificText(), nullable=False)
    __table_args = (
        Index(
            'idx_filereference_foreignkeys',
            'structure_id',
            'structure_type'
        ),
    )
