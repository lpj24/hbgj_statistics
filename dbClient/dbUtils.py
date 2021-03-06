# -*- coding: utf-8 -*-
import pymysql as MySQLdb
from DBUtils.PooledDB import PooledDB
from logging.config import dictConfig
import logging
from conf import LOGGING
import time
import cx_Oracle
import re
import sys
reload(sys)
sys.setdefaultencoding('utf8')


dictConfig(LOGGING)


class DButils(object):

    def __init__(self, dbtype=None, conf=None, *args):
        _return_cursor = MySQLdb.cursors.DictCursor if dict in args else MySQLdb.cursors.Cursor
        if dbtype.lower() == "mysql":
            self._pool = PooledDB(MySQLdb, host=conf["host"], user=conf["user"], passwd=conf["password"],
                                  port=conf["port"], db=conf["database"],
                                  mincached=1, maxcached=20, charset="utf8", use_unicode=True, blocking=True
                                  , maxshared=10, cursorclass=_return_cursor, local_infile=True)
        elif dbtype.lower() == "oracle":
            print cx_Oracle.makedsn(conf["ip"], conf["port"], conf["sid"])
            self._pool = PooledDB(cx_Oracle, user=conf["user"], password=conf["password"],
                                  dsn=cx_Oracle.makedsn(conf["ip"], conf["port"], conf["sid"]),
                                  mincached=1, maxcached=50, maxshared=10, maxusage=0)
        self._conn = self._pool.connection()
        self._cursor = self._conn.cursor()

    @staticmethod
    def _log_str(sql, params):
        sql = sql.strip()
        func = sql.split(' ')[1] if sql.find("update") == 0 else sql.split(' ')[2]
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()) + " update " + func + " :" + str(params)

    def batch_insert(self, sql, params):
        cursor = self._cursor
        try:
            logging.warning(self._log_str(sql, params))
            cursor.executemany(sql, params)
            self._conn.commit()
        except MySQLdb.Error, e:
            warning_time = time.strftime('%Y-%m-%d %H:%M', time.localtime())
            logging.error(warning_time + ":" + str(sql)+"--"+str(e.args[1]))

    def insert(self, sql, params=None):
        cursor = self._cursor
        try:
            logging.warning(self._log_str(sql, params))
            cursor.execute(sql, params)
            # logging.warning(dir(cursor))
            self._conn.commit()
        except MySQLdb.Error, e:
            warning_time = time.strftime('%Y-%m-%d %H:%M', time.localtime())
            logging.error(warning_time + ":" + str(sql) + "--" + str(e.args[1]))

    def query_all(self, sql, params=None):
        cursor = self._cursor
        if params is None:
            cursor.execute(sql)
        else:
            if sql.count("tablename") > 0:
                rx = re.compile("tablename")
                sql, num = rx.subn(params.pop(), sql)
            cursor.execute(sql, params)

        # logging.warning(cursor._executed)
        data = cursor.fetchall()
        return data

    def query_one(self, sql, params=None):
        cursor = self._cursor
        try:
            if params is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, params)
            # logging.warning("execute sql" + cursor._executed)

            data = cursor.fetchone()
        except MySQLdb.OperationalError:
            logging.error("error")
        return data
