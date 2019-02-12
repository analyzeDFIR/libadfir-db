## -*- coding: UTF8 -*-
## models.py
##
## Copyright (c) 2018 analyzeDFIR
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

from datetime import datetime
import re
from sqlalchemy.types import String, Text, NVARCHAR, Integer, TIMESTAMP, Boolean
from sqlalchemy.sql.schema import Column, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base, declared_attr

from .utils import TimestampDefaultExpression, DialectSpecificText

class BaseTableTemplate(object):
    '''
    Base table class
    '''
    __KEY_REGEX_01 = re.compile(r'(.)([A-Z][a-z]+)')
    __KEY_REGEX_02 = re.compile(r'([a-z0-9])([A-Z])')

    @classmethod
    def __convert_key(cls, key):
        '''
        Args:
            key: String => key to convert
        Returns:
            String
            key converted from camel case to snake case
            NOTE:
                Implementation taken from:
                https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case#1176023
        Preconditions:
            key is of type String
        '''
        assert isinstance(key, str)
        return re.sub(cls.__KEY_REGEX_02, r'\1_\2', re.sub(cls.__KEY_REGEX_01, r'\1_\2', key)).lower()

    @declared_attr
    def __tablename__(cls):
        return str(cls.__name__.lower())
    def populate_fields(self, data_dict, overwrite=True):
        '''
        Args:
            data_dict: Dict<String, Any>    => dict containing data to map to fields
            overwrite: Boolean              => whether to overwrite values of current instance
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
        '''
        assert hasattr(data_dict, '__getitem__') and all((isinstance(key, str) for key in data_dict)), 'Data_dict is not of type Dict<String, Any>'
        for key in data_dict:
            converted_key = self.__convert_key(key)
            if hasattr(self, converted_key) and (getattr(self, converted_key) is None or overwrite):
                setattr(self, converted_key, data_dict[key])
        return self

BaseTable = declarative_base(cls=BaseTableTemplate)

class TableMixin(object):
    '''
    Mixin class for (non-view) tables
    '''
    id          = Column(Integer, primary_key=True)
    created_at  = Column(TIMESTAMP(timezone=True), server_default=TimestampDefaultExpression(), index=True)
    __table_args__ = dict(
        mysql_engine='InnoDB',
        mysql_charset='utf8mb4'
    )

class FileLedgerMixin(TableMixin):
    '''
    Parsed file ledger (tracking) table mixin, which
    serves as accounting system for parser
    '''
    file_name               = Column(DialectSpecificText(), nullable=False)
    file_path               = Column(DialectSpecificText(), nullable=False)
    file_size               = Column(Integer, nullable=False)
    md5hash                 = Column(DialectSpecificText())
    sha1hash                = Column(DialectSpecificText())
    sha2hash                = Column(DialectSpecificText())
    modify_time             = Column(TIMESTAMP(timezone=True))
    access_time             = Column(TIMESTAMP(timezone=True))
    create_time             = Column(TIMESTAMP(timezone=True))
    completed               = Column(Boolean, index=True)

class FileLedgerLinkedMixin(object):
    '''
    Mixin for tables linked to fileledger table
    '''
    @declared_attr
    def ledger_id(cls):
        return Column(Integer, ForeignKey('fileledger.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False, index=True)

class SharedStructureTableMixin(object):
    '''
    Mixin for tables storing data present in multiple structures
    '''
    structure_id            = Column(Integer, nullable=False)
    structure_type          = Column(DialectSpecificText(), nullable=False)
    __table_args = (
        Index('idx_filereference_foreignkeys', 'structure_id', 'structure_type'),
    )
