import MySQLdb
from MySQLdb.cursors import DictCursor
from dbutils.pooled_db import PooledDB
from flask import current_app


class _DataBaseMixin:

    def db_pool(self):
        app = getattr(self, 'app', None) or current_app
        configs = app.extensions['database']
        db_pool = PooledDB(
            creator=MySQLdb,
            maxcached=5,
            host=configs.host,
            port=configs.port,
            db=configs.db,
            user=configs.user,
            passwd=configs.passwd,
            charset='utf8',
            cursorclass=DictCursor
        )
        return db_pool

    def connect(self):
        db_pool = self.db_pool()
        try:
            db_conn = db_pool.connection()
            return db_conn
        except KeyError:
            raise RuntimeError('the current application was not configured with database')


class _DataBase(_DataBaseMixin):

    def __init__(self, host, port, db, user, passwd):
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.passwd = passwd


class DataBase(_DataBaseMixin):

    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.state = self.init_app(app)
        else:
            self.state = None

    @staticmethod
    def init_database(config):
        return _DataBase(
            config.get('DATABASE_HOST', '127.0.0.1'),
            config.get('DATABASE_PORT', 3306),
            config.get('DATABASE_NAME'),
            config.get('DATABASE_USER'),
            config.get('DATABASE_PASSWORD'),
        )

    def init_app(self, app):
        state = self.init_database(app.config)
        app.extensions['database'] = state
        return state

    def __getattr__(self, name):
        return getattr(self.state, name, None)
