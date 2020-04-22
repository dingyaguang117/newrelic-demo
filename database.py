from sqlalchemy import Column, Integer, String

from sqlalchemy_helper import SQLAlchemy, Model


db = SQLAlchemy(model_class=Model)
db.configure(uri='mysql://root:123456@127.0.0.1:3306/test')


class User(db.Model):
    __tablename__ = 'user'

    id = Column(Integer, primary_key=True)
    name = Column(String)