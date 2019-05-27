## -*- coding: UTF8 -*-
## manager.py
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

#pylint: disable=R0902
from typing import Optional, Any, Callable, Union

from sqlalchemy import create_engine as sqlalchemy_create_engine, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, scoped_session, Session, Query


class DBManager:
    """Database connection manager.  Handles database connection configuration
    for both standard applications and web servers (using thread-local storage),
    reading from and writing to a database (including transactions), etc.  This
    class essentially acts as a convenience wrapper around a SQLAlchemy Engine
    and Session.
    """

    def __init__(self,
        conn_string: Optional[str] = None,
        metadata: Optional[MetaData] = None,
        session_factory: Optional[Callable[..., Session]] = None,
        session: Optional[Union[session, scoped_session]] = None,
        scoped: bool = False
    ) -> None:
        self.conn_string = conn_string
        self.metadata = metadata
        self.session_factory = session_factory
        self.session = session
        self.scoped_sessions = scoped
        self.engine = None

    @property
    def conn_string(self) -> Optional[str]:
        """Getter for conn_string."""
        return self.__conn_string

    @conn_string.setter
    def conn_string(self, value: Optional[str]) -> None:
        """Setter for conn_string."""
        self.__conn_string = value

    @property
    def engine(self) -> Optional[Engine]:
        """Getter for engine."""
        return self.__engine

    @engine.setter
    def engine(self, value: Optional[Engine]) -> None:
        """Setter for engine."""
        self.__engine = value

    @property
    def metadata(self) -> Optional[MetaData]:
        """Getter for metadata."""
        return self.__metadata

    @metadata.setter
    def metadata(self, value: Optional[MetaData]) -> None:
        """Setter for metadata."""
        self.__metadata = value

    @property
    def session_factory(self) -> Optional[Callable[..., Session]]:
        """Getter for session_factory."""
        return self.__session_factory

    @session_factory.setter
    def session_factory(self, value: Optional[Callable[..., Session]]) -> None:
        """Setter for session_factory."""
        self.__session_factory = value

    @property
    def scoped_sessions(self) -> bool:
        """Getter for scoped_sessions."""
        return self.__scoped_sessions

    @scoped_sessions.setter
    def scoped_sessions(self, value: bool) -> bool:
        """Setter for scoped_sessions."""
        self.__scoped_sessions = value

    @property
    def session(self) -> Optional[Union[Session, scoped_session]]:
        """Getter for session."""
        return self.__session

    @session.setter
    def session(self, value: Optional[Union[Session, scoped_session]]) -> None:
        """Setter for session."""
        self.__session = value

    def create_engine(self,
        conn_string: Optional[str] = None,
        persist: bool = True
    ) -> Optional[Engine]:
        """
        Args:
            conn_string => database connection string
            persist     => whether to persist the database engine to self.engine
        Returns:
            New database connection (SQLAlchemy Engine) using either provided conn_string
            or self.conn_string.
            NOTE:
                If both conn_string and self.conn_string are None then will return None.
        Preconditions:
            N/A
        """
        if conn_string is not None:
            self.conn_string = conn_string
        if self.conn_string is not None:
            engine = sqlalchemy_create_engine(self.conn_string)
            if persist:
                self.engine = engine
            return engine
        return None

    def create_session(self, persist: bool = True) -> Union[Session, scoped_session]:
        """
        Args:
            persist => whether to persist the session
        Returns:
            Either new session object or pre-existing session.
            NOTE:
                If self.session_factory is None, this will throw an AttributeError.
        Preconditions:
            N/A
        """
        if self.scoped_sessions:
            return self.session_factory
        if persist:
            if self.session is None:
                self.session = (self.session_factory)()
            return self.session
        return (self.session_factory)()

    def close_session(self,
        session: Optional[Union[Session, scoped_session]] = None
    ) -> None:
        """
        Args:
            session => session to close if not self.session
        Procedure:
            Closes either the provided session or the current
            session (self.session).
        Preconditions:
            N/A
        """
        if session is not None:
            session.close()
        elif self.scoped_sessions and self.session_factory is not None:
            self.session_factory.remove()
        elif self.session is not None:
            self.session.close()
            self.session = None

    def bootstrap(self, engine: Optional[Engine] = None) -> None:
        """
        Args:
            engine  => the connection engine to use
        Procedure:
            Use a database connection (SQLAlchemy Engine) to
            bootstrap a database with the necessary tables,
            indexes, and (materialized) views.
        Preconditions:
            N/A
        """
        if engine is not None:
            self.engine = engine
        if self.engine is not None and self.metadata is not None:
            self.metadata.create_all(self.engine)

    def initialize(self,
        conn_string: Optional[str] = None,
        metadata: Optional[MetaData] = None,
        bootstrap: bool = False,
        scoped: bool = False,
        create_session: bool = False
    ) -> 'DBManager':
        """
        Args:
            conn_string     => database connection string
            metadata        => database metadata object
            bootstrap       => whether to bootstrap database with tables, indexes,
                               and views
            scoped          => whether to use scoped session objects
            create_session  => whether to create a persisted database session
        Procedure:
            Initialize a database connection using self.conn_string and perform
            various setup tasks such as boostrapping the database with the
            necessary tables, indexes and views, and setting up a
            (scoped) session.
            NOTE:
                See http://docs.sqlalchemy.org/en/latest/orm/contextual.html for
                more information about scoped sessions.
        Preconditions:
            N/A
        """
        if conn_string is not None:
            self.conn_string = conn_string
        self.create_engine()
        if metadata is not None:
            self.metadata = metadata
        if self.engine is not None:
            if bootstrap:
                self.bootstrap()
            if scoped or self.scoped_sessions:
                self.session_factory = scoped_session(
                    sessionmaker(bind=self.engine, autoflush=False)
                )
                self.scoped_sessions = True
            else:
                self.session_factory = sessionmaker(bind=self.engine, autoflush=False)
                self.scoped_sessions = False
            if create_session and not self.scoped_sessions:
                self.create_session()
        return self

    def query(self, model: Any, **kwargs: Any) -> Optional[Query]:
        """
        Args:
            model   => model of table to query
            kwargs  => fields to filter on
        Returns:
            SQLAlchemy Query object with field filters from kwargs applied.
            If applying the filters fails, will return None instead
            of raising error.
        Preconditions:
            N/A
        """
        query = self.session.query(model)
        for arg in kwargs:
            query = query.filter(getattr(model, arg) == kwargs[arg])
        return query

    def add(self,
        record: Any,
        session: Optional[Union[session, scoped_session]] = None,
        commit: bool = False
    ) -> 'DBManager':
        """
        Args:
            record      => record to add to current session
            session     => session to add record to
            commit      => whether to commit and end the transaction block
        Procedure:
            Add record to either provided or current session and commit if specified
            (wrapper around Session.add).
        Preconditions:
            N/A
        """
        if session is None:
            session = self.session
        session.add(record)
        if commit:
            self.commit(session)
        return self

    def delete(self,
        record: Any,
        session: Optional[Union[session, scoped_session]] = None,
        commit: bool = False
    ) -> 'DBManager':
        """
        Args:
            record      => record to add to current session
            session     => session to add record to
            commit      => whether to commit and end the transaction block
        Procedure:
            Delete record using either provided session or current session
            and commit if specified (wrapper around Session.delete).
        Preconditions:
            N/A
        """
        if session is None:
            session = self.session
        session.delete(record)
        if commit:
            self.commit(session)
        return self

    def commit(self,
        session: Optional[Union[session, scoped_session]] = None
    ) -> 'DBManager':
        """
        Args:
            session => session to add record to
        Procedure:
            Commit either provided or current session (wrapper around Session.commit).
        Preconditions:
            N/A
        """
        if session is None:
            session = self.session
        session.commit()
        return self

    def rollback(self,
        session: Optional[Union[session, scoped_session]] = None
    ) -> 'DBManager':
        """
        Args:
            session => session to add record to
        Procedure:
            Rollback either provided or current session (wrapper around Session.rollback).
        Preconditions:
            N/A
        """
        if session is None:
            session = self.session
        session.rollback()
        return self
