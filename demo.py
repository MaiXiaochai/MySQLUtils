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


if __name__ == '__main__':
    main()
