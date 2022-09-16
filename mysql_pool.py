"""
------------------------------------------
@File       : mysql_pool.py
@CreatedOn  : 2022/9/16 9:52
------------------------------------------
    MySQL 连接池
    参考文章：https://blog.csdn.net/weixin_41447636/article/details/110453039
"""
import pymysql
from dbutils.pooled_db import PooledDB


class MySQLUtils:
    """MySQL 基本功能封装 """

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

    @staticmethod
    def gen_pool():
        pool = PooledDB(
            creator=pymysql,  # 使用链接数据库的模块
            mincached=0,  # 初始化连接池时创建的连接数。默认为0，即初始化时不创建连接(建议默认0，假如非0的话，在某些数据库不可用时，整个项目会启动不了)
            maxcached=0,  # 池中空闲连接的最大数量。默认为0，即无最大数量限制(建议默认)
            maxshared=0,  # 池中共享连接的最大数量。默认为0，即每个连接都是专用的，不可共享(不常用，建议默认)
            maxconnections=0,  # 被允许的最大连接数。默认为0，无最大数量限制
            blocking=True,  # 连接数达到最大时，新连接是否可阻塞。默认False，即达到最大连接数时，再取新连接将会报错。(建议True，达到最大连接数时，新连接阻塞，等待连接数减少再连接)
            maxusage=0,  # 连接的最大使用次数。默认0，即无使用次数限制。(建议默认)
            reset=True,  # 当连接返回到池中时，重置连接的方式。默认True，总是执行回滚
            ping=0,  # 确定何时使用ping()检查连接。默认1，即当连接被取走，做一次ping操作。0是从不ping，1是默认，2是当该连接创建游标时ping，4是执行sql语句时ping，7是总是ping
            host=self.swmconn['host'],
            port=self.swmconn['port'],
            user=self.swmconn['user'],
            passwd=self.swmconn['passwd'],
            db=self.swmconn['db'],
            charset=self.swmconn['charset']
        )
        return pool

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

    def has_table(self, table_name: str) -> bool:
        """
            该用户下是否存在表table_name
        """
        sql = "SELECT count(*) total FROM information_schema.TABLES WHERE table_name =%(table_name)s"
        arg = {'table_name': table_name}
        return self.fetchone(sql, arg).get('total') == 1

    def exist_data_by_kw(self, table_name: str, data: dict):
        """表table_name中是否存在where key = value的数据"""
        k, = data

        exist_sql = "select * from {0} where {1} = %({1})s"
        sql = exist_sql.format(table_name, k)

        f_data = self.fetchone(sql, data)
        result = True if f_data else False

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
