class DBConnection:
    def __init__(self):
        self.dbconn = None
        self.cursor = None

    def commit(self):
        self.dbconn.commit()

    def __enter__(self):
        try:
            if not self.cursor:
                if not self.dbconn:
                    self.dbconn = create_dbconn_mysql()
                self.cursor = self.dbconn.cursor()
        except:
            raise Exception('db connection failed')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.dbconn:
                if self.cursor:
                    self.cursor.execute('UNLOCK TABLES;')
                    self.cursor.close()
                self.dbconn.close()
        except:
            raise Exception('db close failed')
        finally:
            self.dbconn = None
            self.cursor = None
