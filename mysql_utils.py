# -*- coding: utf-8 -*-

"""
--------------------------------------
@File       : mysql_utils.py
@Author     : maixiaochai
@Email      : maixiaochai@outlook.com
@CreatedOn  : 2020/11/9 22:23
--------------------------------------

"""
from pymysql import connect
from pymysql.cursors import DictCursor


class MySQLUtils:
    """MySQL 基本功能封装 """

    __exist_sql = " select * from {0} where {1} = %({1})s"

    def __init__(self,
                 host: str,
                 port: int,
                 user: str,
                 passwd: str,
                 db: str):

        self.conn = connect(
            host=host,
            port=port,
            user=user,
            passwd=passwd,
            db=db,
            charset='utf8',  # utf8mb4 是utf8的超集
            cursorclass=DictCursor)  # 返回类字典类型游标
        self.cur = self.conn.cursor()

    def execute(self, sql, args=None):
        self.cur.execute(sql, args)

    def executemany(self, sql, args):
        self.cur.executemany(sql, args)
        self.commit()

    def fetchall(self, sql, args=None):
        self.execute(sql, args)
        result = self.cur.fetchall()

        return result

    def fetchone(self, sql, args=None):
        self.execute(sql, args)
        result = self.cur.fetchone()

        return result

    def exist_data_by_kw(self, table_name: str, data: dict):
        """表table_name中是否存在where key = value的数据"""
        k, = data
        sql = self.__exist_sql.format(table_name, k)
        f_data = self.fetchone(sql, data)

        result = False
        if f_data:
            result = True

        return result

    @property
    def rowcount(self):
        """受影响的行数"""
        count = self.cur.rowcount

        return count

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        try:
            self.conn.close()
        except Exception as err:
            print(str(err))

    def __del__(self):
        self.close()
