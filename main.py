import tornado.ioloop
import tornado.web

from sqlalchemy_helper import SessionMixin
from database import db, User


class MainHandler(tornado.web.RequestHandler, SessionMixin):
    def get(self):
        user = db.session.query(User).first()
        print(user)
        return self.finish({'status': 'ok'})


class MainHandler2(tornado.web.RequestHandler, SessionMixin):
    def get(self):
        user = db.session.query(User).first()
        print(user)
        return self.write({'status': 'ok'})

def make_app():
    return tornado.web.Application([
        (r"/not-work", MainHandler),
        (r"/work", MainHandler2),
    ])

if __name__ == "__main__":
    app = make_app()
    app.listen(9999)
    tornado.ioloop.IOLoop.current().start()