# -*- encoding: utf-8 -*-

"""
------------------------------------------
@File       : demo.py
@Author     : maixiaochai
@Email      : maixiaochai@outlook.com
@CreatedOn  : 2021/1/18 19:47
------------------------------------------
"""
from mysql_utils import MySQLUtils
from mysql_pool import MySQLConnectionPool


def main():
    cfg = {
        'host': '192.168.158.11',
        'port': 3306,
        'user': 'maixiaochai_u',
        'passwd': 'maixiaochai_p',
        'db': 'maixiaochai_db'
    }

    db = MySQLUtils(**cfg)
    result = db.has_table('maixiaochai_t')
    print(result)


def demo_mysql_connection_pool():
    cfg = {
        'host': '192.168.x.x',
        'port': 3306,
        'user': 'maxiaochai',
        'passwd': 'maxiaochai',
        'db': 'maxiaochai'
    }

    db = MySQLConnectionPool(**cfg)
    table_name = 'maxiaochai'
    sql = f"select count(*) total from {table_name}"
    result = db.fetchone(sql)
    number = result.get('total')
    print(result)
    print(number, type(number))


if __name__ == '__main__':
    # main()
    demo_mysql_connection_pool()
