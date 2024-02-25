from todoist.utils import BaseModel, Common


class UserModel(BaseModel):

    def get_users(self, params):
        params = params.__dict__
        sql_str = '''
            SELECT id, um, password, name, is_admin, creat_time
            FROM user
        '''
        filter_fields = ['um', 'password', 'name', 'is_admin', 'creat_time']
        where_str, where_values = Common.parse_where_str(filter_fields, params)
        limit_str = Common.parse_limit_str(params)
        self.conn()
        self.execute((sql_str + where_str + limit_str), where_values)
        data = self.cursor.fetchall()
        page = int(params.get('page', 1))
        per_page = int(params.get('size', 20))
        page_info = Common.get_page_info(self.cursor, sql_str + where_str, where_values, truncate=True, page=page, per_page=per_page)
        self.close()
        page_info.update({'data': data})
        return page_info

    def get_user_by_id(self, user_id):
        sql_str = '''
            SELECT id, um, password, name, is_admin, creat_time
            FROM user
            WHERE id = %s
        '''
        self.conn()
        self.execute(sql_str, (user_id,))
        data = self.cursor.fetchone()
        self.close()
        return data

    def get_user_by_um(self, um):
        sql_str = '''
            SELECT id, um, password, name, is_admin, creat_time
            FROM user
            WHERE um = %s
        '''
        self.conn()
        self.execute(sql_str, (um,))
        data = self.cursor.fetchone()
        self.close()
        return data

    def add_user(self, user):
        sql_str = '''
            INSERT INTO user (um, password, name, is_admin, creat_time)
            VALUES (%s, %s, %s, %s, %s)
        '''
        self.conn()
        self.execute(sql_str, (user.um, user.password, user.name, user.is_admin, user.creat_time))
        self.commit()
        user_id = self.cursor.lastrowid  # 新增数据的ID
        self.close()
        return user_id

    def delete_user(self, user_id):
        sql_str = '''
            DELETE FROM user WHERE id = %s
        '''
        self.conn()
        self.execute(sql_str, (user_id,))
        self.commit()
        row = self.cursor.rowcount  # 执行的数量
        self.close()
        return row

    def update_user(self, user):
        sql_str = '''
            UPDATE user SET password = %s, name = %s, is_admin = %s WHERE id = %s
        '''
        self.conn()
        self.execute(sql_str, (user.password, user.name, user.is_admin, user.id))
        self.commit()
        row = self.cursor.rowcount  # 执行的数量
        self.close()
        return row
