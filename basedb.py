import math
from functools import lru_cache

import aiomysql


async def _create_dbpool():
    dbpool = await aiomysql.create_pool(
        host=CONF.get_conf('DATABASE', 'host'),
        port=int(CONF.get_conf('DATABASE', 'port')),
        db=CONF.get_conf('DATABASE', 'name'),
        user=CONF.get_conf('DATABASE', 'user'),
        password=CONF.get_secconf('DATABASE', 'passwd')
    )
    return dbpool


@lru_cache
async def get_dbpool():
    return await _create_dbpool()


class BaseModel:

    def __init__(self):
        self.__conn = None
        self.cur = None

    @classmethod
    async def _initialize_pool(cls):
        if not hasattr(cls, '__pool'):
            cls.__pool = await get_dbpool()

    async def conn(self):
        await self._initialize_pool()
        if not self.cur:
            if not self.__conn:
                self.__conn = await self.__pool.acquire()
            self.cur = await self.__conn.cursor()

    async def begin(self):
        await self.__conn.begin()

    async def rollback(self):
        await self.__conn.rollback()

    async def execute(self, sql_str, values=None):
        try:
            await self.cur.execute(sql_str, values)
        except Exception as e:
            await self.rollback()
            await self.close()
            raise ConnectionAbortedError(str(e))

    async def executemany(self, sql_str, values=None):
        try:
            await self.cur.executemany(sql_str, values)
        except Exception as e:
            await self.rollback()
            await self.close()
            raise ConnectionAbortedError(str(e))

    async def commit(self):
        try:
            await self.__conn.commit()
        except Exception as e:
            await self.rollback()
            raise ConnectionAbortedError(str(e))

    async def close(self):
        try:
            if self.cur:
                await self.cur.execute('UNLOCK TABLES;')
                await self.cur.close()
        except Exception as e:
            raise ConnectionAbortedError(str(e))
        finally:
            self.__pool.release(self.__conn)
            self.cur = None
            self.__conn = None


class Common:
    @staticmethod
    async def parse_limit_str(page_info=None):
        if page_info is None:
            page_info = {}
        page = int(page_info.get('page', 1))
        page_size = int(page_info.get('rows', 20))
        limit_str = f' LIMIT {(page - 1) * page_size}, {page_size} '
        return limit_str

    @staticmethod
    async def parse_update_str(table, p_key, p_id, update_dict):
        sql_str = f' UPDATE {table} SET '
        temp_str = []
        sql_values = []
        for key, value in update_dict.items():
            temp_str.append(key + ' = %s ')
            sql_values.append(value)
        sql_str += ', '.join(r for r in temp_str) + ' WHERE ' + p_key + ' = %s '
        sql_values.append(p_id)
        return sql_str, sql_values

    @staticmethod
    async def parse_where_str(filter_fields, request_data):
        if not isinstance(filter_fields, (tuple, list)):
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
    async def parse_where_like_str(filter_fields, request_data):
        if not isinstance(filter_fields, (tuple, list)):
            filter_fields = (filter_fields,)
        where_str = ' WHERE 1 = %s '
        where_values = [1]
        for key in filter_fields:
            value = request_data.get(key)
            if value:
                where_str += ' AND ' + key + ' LIKE %s '
                where_values.append(f'%%%%{value}%%%%')
        if not where_values:
            where_values = None
        return where_str, where_values

    @staticmethod
    async def get_page_info(cursor, sql_str, where_values=None, truncate=False, page=1, per_page=20):
        page = int(page)
        per_page = int(per_page)

        if truncate:
            if 'GROUP BY' in sql_str:
                sql_str = f'SELECT COUNT(*) total FROM ({sql_str}) AS TEMP'
            else:
                sql_str = re.sub(r'SELECT[\s\S]*?FROM', 'SELECT COUNT(*) total FROM', sql_str, count=1)

        # 从原始 SQL 删除 ORDER BY 和 LIMIT （用于计算总数）
        if 'ORDER BY' in sql_str:
            sql_str = sql_str[:sql_str.find('ORDER BY')]
        if 'LIMIT' in sql_str:
            sql_str = sql_str[:sql_str.find('LIMIT')]

        # 执行查询以获得总记录数
        if where_values:
            await cursor.execute(sql_str, where_values)
        else:
            await cursor.execute(sql_str)
        total = await cursor.fetchone()['total']

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
