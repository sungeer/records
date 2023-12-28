class Common:
    @staticmethod
    def parse_limit_str(page_info=None):
        if page_info is None:
            page_info = {}
        page_info = int(page_info.get('page', 1))
        page_size = int(page_info.get('rows', 100))
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
    def get_page_info(cursor, sql_str, where_values=None, trancate=False, page=1, page_size=100):
        page_size = int(page_size)
        if sql_str.find('ORDER BY') > 0:
            sql_str = sql_str[:sql_str.find('ORDER BY')]
        if sql_str.find('LIMIT') > 0:
            sql_str = sql_str[:sql_str.find('LIMIT')]
        if trancate:
            if sql_str.find('GROUP BY') > 0:
                sql_str = 'SELECT COUNT(*) total FROM (%s) AS TEMP' % sql_str
            else:
                sql_str = re.sub(r'SELECT[\s\S]*?FROM', 'SELECT COUNT(*) total FROM', sql_str, count=1)
        if where_values:
            cursor.execute(sql_str, where_values)
        else:
            cursor.execute(sql_str)
        total = cursor.fetchone()
        page_info = {
            'page': page,
            'total': total['total'] // page_size + 1,
            'records': total['total']
        }
        return page_info
