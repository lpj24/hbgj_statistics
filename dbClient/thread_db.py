import pymysql as MySQLdb
import cx_Oracle
import conf


class DButils(object):
    def __init__(self, dbtype, conf):
        if dbtype.lower() == "mysql":
            self._cursor = MySQLdb.connect(host=conf["host"], user=conf["user"], passwd=conf["password"], db=conf["database"], charset="utf8").cursor()
        elif dbtype.lower() == "oracle":
            self._cursor = cx_Oracle.connect(user=conf["user"], password=conf["password"],
                                        dsn=cx_Oracle.makedsn(conf["ip"], conf["port"], conf["sid"])).cursor()

    def query_all(self, sql, params=None):
        self._execute(sql, params)
        data = self._cursor.fetchall()
        return data

    def query_one(self, sql, params=None):
        self._execute(sql, params)
        data = self._cursor.fetchone()
        return data

    def _execute(self, sql, params=None):
        if params:
            self._cursor.execute(sql, params)
        else:
            self._cursor.execute(sql)


class DBCliThread(object):
    def __init__(self):
        self._source_db = conf.source_db

    @property
    def sourcedb_cli(self):
        return DButils("mysql", self._source_db)