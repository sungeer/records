import math
import re

import MySQLdb
from MySQLdb.cursors import DictCursor  # python -m pip install mysqlclient
from dbutils.pooled_db import PooledDB  # python -m pip install DBUtils

from fairy.config import db_settings

dbpool = PooledDB(
    creator=MySQLdb,
    maxcached=5,
    host=db_settings.DB_HOST,
    port=db_settings.DB_PROT,
    db=db_settings.DB_NAME,
    user=db_settings.DB_USER,
    passwd=db_settings.DB_PASS,
    charset='utf8mb4',
    cursorclass=DictCursor
)


def create_dbconn_mysql():
    conn = dbpool.connection()
    return conn


def cur():
    conn = MySQLdb.connect(
        host=db_settings.DB_HOST,
        port=db_settings.DB_PROT,
        db=db_settings.DB_NAME,
        user=db_settings.DB_USER,
        passwd=db_settings.DB_PASS,
        charset='utf8mb4',
        cursorclass=DictCursor
    )
    conn.text_factory = str
    return conn


class BaseModel:
    def __init__(self):
        self._conn = None
        self.cursor = None

    def conn(self):
        if not self.cursor:
            if not self._conn:
                self._conn = create_dbconn_mysql()
            self.cursor = self._conn.cursor()

    def rollback(self):
        if self._conn:
            self._conn.rollback()

    def commit(self):
        try:
            self._conn.commit()
        except Exception as e:
            self.rollback()
            raise ConnectionAbortedError(str(e))

    def begin(self):
        if self._conn:
            self._conn.begin()

    def close(self):
        try:
            if self._conn:
                if self.cursor:
                    self.cursor.execute('UNLOCK TABLES;')
                    self.cursor.close()
                self._conn.close()
        finally:
            self.cursor = None
            self._conn = None

    def execute(self, sql_str, values=None):
        try:
            self.cursor.execute(sql_str, values)
        except Exception as e:
            self.rollback()
            self.close()
            raise ConnectionAbortedError(str(e))

    def executemany(self, sql_str, values=None):
        try:
            self.cursor.executemany(sql_str, values)
        except Exception as e:
            self.rollback()
            self.close()
            raise ConnectionAbortedError(str(e))


class DBConnection:
    def __init__(self):
        self._conn = None
        self.cursor = None

    def commit(self):
        self._conn.commit()

    def __enter__(self):
        if not self.cursor:
            if not self._conn:
                self._conn = create_dbconn_mysql()
            self.cursor = self._conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self._conn:
                if self.cursor:
                    self.cursor.execute('UNLOCK TABLES;')
                    self.cursor.close()
                self._conn.close()
        finally:
            self._conn = None
            self.cursor = None


class Common:
    @staticmethod
    def parse_limit_str(page_info=None):
        if page_info is None:
            page_info = {}
        page = int(page_info.get('page', 1))
        page_size = int(page_info.get('rows', 20))
        limit_str = ' LIMIT %s, %s ' % ((page - 1) * page_size, page_size)
        return limit_str

    @staticmethod
    def parse_update_str(table, p_key, p_id, update_dict):
        sql_str = ' UPDATE %s SET ' % (table,)
        temp_str = []
        sql_values = []
        for key, value in update_dict.items():
            temp_str.append(key + ' = %s ')
            sql_values.append(value)
        sql_str += ', '.join(r for r in temp_str) + ' WHERE ' + p_key + ' = %s '
        sql_values.append(p_id)
        return sql_str, sql_values

    @staticmethod
    def parse_where_str(filter_fields, request_data):
        if not isinstance(filter_fields, tuple) and not isinstance(filter_fields, list):
            filter_fields = (filter_fields,)
        where_str = ' WHERE 1 = %s '
        where_values = [1]
        for key in filter_fields:
            value = request_data.get(key)
            if value:
                where_str += ' AND ' + key + ' = %s '
                where_values.append(value)
        if not where_values:
            where_values = None
        return where_str, where_values

    @staticmethod
    def parse_where_like_str(filter_fields, request_data):
        if not isinstance(filter_fields, tuple) and not isinstance(filter_fields, list):
            filter_fields = (filter_fields,)
        where_str = ' WHERE 1 = %s '
        where_values = [1]
        for key in filter_fields:
            value = request_data.get(key)
            if value:
                where_str += ' AND ' + key + ' LIKE %s '
                where_values.append('%%%%%s%%%%' % value)
        if not where_values:
            where_values = None
        return where_str, where_values

    @staticmethod
    def get_page_info(cursor, sql_str, where_values=None, truncate=False, page=1, per_page=20):
        page = int(page)
        per_page = int(per_page)

        if truncate:
            if 'GROUP BY' in sql_str:
                sql_str = 'SELECT COUNT(*) total FROM (%s) AS TEMP' % sql_str
            else:
                sql_str = re.sub(r'SELECT[\s\S]*?FROM', 'SELECT COUNT(*) total FROM', sql_str, count=1)

        # 从原始 SQL 删除 ORDER BY 和 LIMIT （用于计算总数）
        if 'ORDER BY' in sql_str:
            sql_str = sql_str[:sql_str.find('ORDER BY')]
        if 'LIMIT' in sql_str:
            sql_str = sql_str[:sql_str.find('LIMIT')]

        # 执行查询以获得总记录数
        if where_values:
            cursor.execute(sql_str, where_values)
        else:
            cursor.execute(sql_str)
        total = cursor.fetchone()['total']

        # 计算分页信息
        pages = math.ceil(total / per_page)
        next_num = page + 1 if page < pages else None
        has_next = page < pages
        prev_num = page - 1 if page > 1 else None
        has_prev = page > 1

        # 构建并返回分页信息字典
        page_info = {
            'page': page,
            'per_page': per_page,  # 每页显示的记录数
            'pages': pages,  # 总页数
            'total': total,
            'next_num': next_num,
            'has_next': has_next,
            'prev_num': prev_num,
            'has_prev': has_prev
        }
        return page_info
