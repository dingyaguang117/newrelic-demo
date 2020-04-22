# -*- coding: utf-8 -*-
# https://github.com/dingyaguang117/tornado-sqlalchemy

import multiprocessing
import time
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import Callable, Optional

from sqlalchemy import create_engine, orm
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.orm.exc import UnmappedClassError
from sqlalchemy.orm.session import Session
from tornado.concurrent import Future, chain_future
from tornado.ioloop import IOLoop
from tornado.web import Application
from .model import BindMeta, Model
from context import get_context_id, set_request_id, get_request_id

__all__ = ('as_future', 'SessionMixin', 'set_max_workers', 'SQLAlchemy')


class MissingDatabaseSettingError(Exception):
    pass


class _AsyncExecution:
    """Tiny wrapper around ThreadPoolExecutor. This class is not meant to be
    instantiated externally, but internally we just use it as a wrapper around
    ThreadPoolExecutor so we can control the pool size and make the
    `as_future` function public.
    """

    def __init__(self, max_workers: Optional[int] = None):
        self._max_workers = (
            max_workers or multiprocessing.cpu_count()
        )  # type: int
        self._pool = None  # type: Optional[Executor]

    def set_max_workers(self, count: int):
        if self._pool:
            self._pool.shutdown(wait=True)

        self._max_workers = count
        self._pool = ThreadPoolExecutor(max_workers=self._max_workers)

    def as_future(self, query: Callable) -> Future:
        # concurrent.futures.Future is not compatible with the "new style"
        # asyncio Future, and awaiting on such "old-style" futures does not
        # work.
        #
        # tornado includes a `run_in_executor` function to help with this
        # problem, but it's only included in version 5+. Hence, we copy a
        # little bit of code here to handle this incompatibility.

        if not self._pool:
            self._pool = ThreadPoolExecutor(max_workers=self._max_workers)

        old_future = self._pool.submit(query)
        new_future = Future()  # type: Future

        IOLoop.current().add_future(
            old_future, lambda f: chain_future(f, new_future)
        )

        return new_future


class SessionMixin:
    _session = None  # type: Optional[Session]
    application = None  # type: Optional[Application]

    def prepare(self):
        # set request_id for scoped_session
        # 取 request 对象 id 后三位 + 时间戳
        request_id = '%03d-%d' % (id(self.request) % 1000, time.time() * 100000, )
        set_request_id(request_id)

    def on_finish(self):
        next_on_finish = None

        try:
            next_on_finish = super(SessionMixin, self).on_finish
        except AttributeError:
            pass

        # close and remove session created by db.session
        db = self.application.settings.get('db')
        if db:
            db.session.remove()

        if next_on_finish:
            next_on_finish()


_async_exec = _AsyncExecution()

as_future = _async_exec.as_future

set_max_workers = _async_exec.set_max_workers


class SessionEx(Session):
    """The SessionEx extends the default session system with bind selection.
    """

    def __init__(self, db, autocommit=False, autoflush=True, **options):
        self.db = db
        bind = options.pop('bind', None) or db.engine
        binds = options.pop('binds', db.get_binds())

        super().__init__(
            autocommit=autocommit,
            autoflush=autoflush,
            bind=bind,
            binds=binds,
            **options
        )

    def get_bind(self, mapper=None, clause=None):
        """Return the engine or connection for a given model or
        table, using the ``__bind_key__`` if it is set.
        """
        # mapper is None if someone tries to just get a connection
        if mapper is not None:
            try:
                # SA >= 1.3
                persist_selectable = mapper.persist_selectable
            except AttributeError:
                # SA < 1.3
                persist_selectable = mapper.mapped_table

            info = getattr(persist_selectable, 'info', {})
            bind_key = info.get('bind_key')
            if bind_key is not None:
                return self.db.get_engine(bind=bind_key)
        return super().get_bind(mapper, clause)


class SQLAlchemy:
    def __init__(
        self, uri=None, binds=None, session_options=None, engine_options=None, model_class=Model
    ):
        self.Model: Model = self.make_declarative_base(model_class)
        self._engines = {}
        self.session: Session = None

        self.configure(
            uri=uri,
            binds=binds,
            session_options=session_options,
            engine_options=engine_options,
        )

    def configure(
        self, uri=None, binds=None, session_options=None, engine_options=None
    ):
        self.uri = uri
        self.binds = binds or {}
        self._engine_options = engine_options or {}

        self.sessionmaker = sessionmaker(
            class_=SessionEx, db=self, **(session_options or {})
        )
        self.session = scoped_session(
            self.sessionmaker, scopefunc=get_context_id
        )

    @property
    def engine(self):
        return self.get_engine()

    @property
    def metadata(self):
        return self.Model.metadata

    def create_engine(self, bind=None):
        if not self.uri and not self.binds:
            raise MissingDatabaseSettingError()

        if bind is None:
            uri = self.uri
        else:
            if bind not in self.binds:
                raise RuntimeError('bind {} undefined.'.format(bind))
            uri = self.binds[bind]

        return create_engine(uri, **self._engine_options)

    def get_engine(self, bind=None):
        """Returns a specific engine. cached in self._engines """
        engine = self._engines.get(bind)

        if engine is None:
            engine = self.create_engine(bind)
            self._engines[bind] = engine

        return engine

    def get_tables_for_bind(self, bind=None):
        """Returns a list of all tables relevant for a bind."""
        result = []
        for table in self.Model.metadata.tables.values():
            if table.info.get('bind_key') == bind:
                result.append(table)
        return result

    def get_binds(self):
        """Returns a dictionary with a table->engine mapping.

        This is suitable for use of sessionmaker(binds=db.get_binds()).
        """
        binds = [None] + list(self.binds)
        retval = {}
        for bind in binds:
            engine = self.get_engine(bind)
            tables = self.get_tables_for_bind(bind)
            retval.update(dict((table, engine) for table in tables))
        return retval

    def _execute_for_all_tables(self, bind, operation, skip_tables=False):
        if bind == '__all__':
            binds = [None] + list(self.binds)
        elif isinstance(bind, str) or bind is None:
            binds = [bind]
        else:
            binds = bind

        for bind in binds:
            extra = {}
            if not skip_tables:
                tables = self.get_tables_for_bind(bind)
                extra['tables'] = tables
            op = getattr(self.Model.metadata, operation)
            op(bind=self.get_engine(bind), **extra)

    def create_all(self, bind='__all__'):
        """Creates all tables.
        """
        self._execute_for_all_tables(bind, 'create_all')

    def drop_all(self, bind='__all__'):
        """Drops all tables.
        """
        self._execute_for_all_tables(bind, 'drop_all')

    def make_declarative_base(self, model):

        if not isinstance(model, DeclarativeMeta):
            model = declarative_base(cls=model, metaclass=BindMeta)

        model.query = _QueryProperty(self)
        return model


class _QueryProperty(object):
    def __init__(self, sa):
        self.sa = sa

    def __get__(self, obj, type):
        try:
            mapper = orm.class_mapper(type)
            if mapper:
                return orm.Query(mapper, session=self.sa.session())
        except UnmappedClassError:
            return None
