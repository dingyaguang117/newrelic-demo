from sqlalchemy import inspect
from sqlalchemy.orm import Query
from sqlalchemy.ext.declarative import DeclarativeMeta
from ._compat import to_str


class BindMeta(DeclarativeMeta):
    def __init__(cls, name, bases, d):
        bind_key = d.pop('__bind_key__', None) or getattr(
            cls, '__bind_key__', None
        )

        super(BindMeta, cls).__init__(name, bases, d)

        if (
            bind_key is not None
            and getattr(cls, '__table__', None) is not None
        ):
            cls.__table__.info['bind_key'] = bind_key


class Model(object):
    """Base class for SQLAlchemy declarative base model.
    To define models, subclass :attr:`db.Model <SQLAlchemy.Model>`, not this
    class. To customize ``db.Model``, subclass this and pass it as
    ``model_class`` to :class:`SQLAlchemy`.
    """

    #: Query class used by :attr:`query`. Defaults to
    # :class:`SQLAlchemy.Query`, which defaults to :class:`BaseQuery`.
    query_class = None

    #: Convenience property to query the database for instances of this model
    # using the current session. Equivalent to ``db.session.query(Model)``
    # unless :attr:`query_class` has been changed.
    query: Query = None

    @classmethod
    def findone(cls, **kwargs):
        return cls.query.filter_by(**kwargs).first()

    @classmethod
    def latest(cls, **kwargs):
        return cls.query.filter_by(**kwargs).order_by(cls.id.desc()).first()

    @classmethod
    def getall(cls, **kwargs):
        return cls.query.filter_by(**kwargs)

    def delete(self):
        pass

    @classmethod
    def _all_fields(cls):
        return [c.name for c in cls.__table__.columns]

    @classmethod
    def show_fields(cls):
        return cls._all_fields()

    def to_dict(self, fields=None):
        """
        将 model 转换为字典
        :param fields: 可以为:
           1. None: show_fields 定义的字段
           2. 'all': 所有字段
           3. ['字段', ..]: 自定义字段
        :return: 转换后的字典
        """
        if fields is None:
            fields = self.show_fields()
        elif fields == 'all':
            fields = self._all_fields()
        elif not isinstance(fields, list):
            raise Exception('to_dict parameter `fields` must be "all", None or list of fields')
        return dict((c, getattr(self, c)) for c in fields)

    @classmethod
    def filter_fields(cls, data):
        return {k: v for k, v in data.items() if hasattr(cls, k)}

    def __repr__(self):
        identity = inspect(self).identity
        if identity is None:
            pk = "(transient {})".format(id(self))
        else:
            pk = ', '.join(to_str(value) for value in identity)
        return '<{} {}>'.format(type(self).__name__, pk)

