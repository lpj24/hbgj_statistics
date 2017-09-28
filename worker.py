# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import mmh3
from pybloom import BloomFilter
SEEDS = [43, 47]
bit_size = 1400000


def update_focus_newuser(days=0):
    """更新航班关注新用户, hbdt_focus_newusers_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    query_file = start_date + "_hbdt_focus.dat"
    query_id = list()
    with open("C:\\Users\\Administrator\\Desktop\\" + query_file) as hbdt_focus_data:
        for hbdt_data in hbdt_focus_data:
            try:
                (userid, phoneid, phone, token, flyid, focusdate
                 , platform, ordertype) = hbdt_data.strip().split("\t")
            except Exception:
                continue

            phone_id = str(userid) + '_' + str(phoneid)
            query_date = focusdate.split(" ")[0]
            if query_date == "None":
                continue
            query_id.append(phone_id)
    focus_pv = len(query_id)
    focus_uv = len(set(query_id))
    his_focus_id = DBCli().redis_dt_cli.smembers("hbdt_focus_his_uid")
    focus_newuser = len(set(query_id).difference(his_focus_id))
    print focus_uv, focus_pv, focus_newuser
    # DBCli().targetdb_cli.insert(insert_sql, [start_date, focus_uv, focus_pv, focus_newuser])
    # for focus_id in query_id:
    #     DBCli().redis_dt_cli.sadd("hbdt_focus_his_uid", focus_id)


def update_focus_newuser_bit(days=0):
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    query_file = start_date + "_hbdt_focus.dat"
    query_id = list()
    g = BloomFilter(capacity=1400000, error_rate=0.001)
    focus_newuser = []
    bloom_file = open("bloom.txt", 'rb')
    b_f = BloomFilter.fromfile(bloom_file)
    with open("C:\\Users\\Administrator\\Desktop\\" + query_file) as hbdt_focus_data:
        for hbdt_data in hbdt_focus_data:
            try:
                (userid, phoneid, phone, token, flyid, focusdate
                 , platform, ordertype) = hbdt_data.strip().split("\t")
            except Exception:
                continue

            phone_id = str(userid) + '_' + str(phoneid)
            query_date = focusdate.split(" ")[0]
            if query_date == "None":
                continue
            query_id.append(phone_id)
            if not (phone_id in b_f):
                g.add(phone_id)
                focus_newuser.append(phone_id)
    focus_pv = len(query_id)
    focus_uv = len(set(query_id))
    print focus_uv, focus_pv, len(set(focus_newuser))
    bloom_file.close()
    with open("./last.txt", "wb") as bloom_f:
        g.tofile(bloom_f)

    g_f = g.fromfile(open("last.txt", 'rb'))
    new_bloom = g_f.union(b_f)
    with open("./bloom.txt", "wb") as bloom_f:
        new_bloom.tofile(bloom_f)


def update_focus_newuser_bit_2(days=0):
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    query_file = start_date + "_hbdt_focus.dat"
    query_id = list()
    last_bf = BloomFilter(capacity=1400000, error_rate=0.001)
    focus_newuser = []
    with open("bloom.txt", 'rb') as bloom_file:
        b_f = BloomFilter.fromfile(bloom_file)

    with open("bit_" + query_file, 'rb') as last_f:
        last_bf = BloomFilter.fromfile(last_f)

    with open("C:\\Users\\Administrator\\Desktop\\" + query_file) as hbdt_focus_data:
        for hbdt_data in hbdt_focus_data:
            try:
                (userid, phoneid, phone, token, flyid, focusdate
                 , platform, ordertype) = hbdt_data.strip().split("\t")
            except Exception:
                continue

            phone_id = str(userid) + '_' + str(phoneid)
            query_date = focusdate.split(" ")[0]
            if query_date == "None":
                continue
            query_id.append(phone_id)
            if not (phone_id in b_f):
                focus_newuser.append(phone_id)
    focus_pv = len(query_id)
    focus_uv = len(set(query_id))
    print focus_uv, focus_pv, len(set(focus_newuser))
    new_bloom = last_bf.union(b_f)
    with open("./bloom.txt", "wb") as bloom_f:
        new_bloom.tofile(bloom_f)


def covent_bit_file(days=1):
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    query_file = start_date + "_hbdt_focus.dat"
    g_f = BloomFilter(capacity=1400000, error_rate=0.001)
    with open("C:\\Users\\Administrator\\Desktop\\" + query_file) as hbdt_focus_data:
        for hbdt_data in hbdt_focus_data:
            try:
                (userid, phoneid, phone, token, flyid, focusdate
                 , platform, ordertype) = hbdt_data.strip().split("\t")
            except Exception:
                continue

            phone_id = str(userid) + '_' + str(phoneid)
            query_date = focusdate.split(" ")[0]
            if query_date == "None":
                continue
            g_f.add(phone_id)
    with open("bit_" + query_file, 'wb') as bit_file:
        g_f.tofile(bit_file)


def collect_his_phone_uid():

    with open("C:\\Users\\Administrator\\Desktop\\hbdt_focus_platform.dat") as hbdt_focus_data:
        for hbdt_data in hbdt_focus_data:
            try:
                (userid, phoneid, phone, token, flyid, focusdate, flydate
                 , createtime, platform, ordertype) = hbdt_data.strip().split("\t")
            except Exception:
                continue

            phone_id = str(userid) + '_' + str(phoneid)
            query_date = createtime.split(" ")[0] if createtime.split(" ")[0] != "None" else focusdate.split(" ")[0]
            if query_date == "None":
                continue
            DBCli().redis_dt_cli.sadd("hbdt_focus_his_uid", phone_id)


def collect_his_phone_uid_bit():
    f = BloomFilter(capacity=1400000, error_rate=0.001)

    with open("C:\\Users\\Administrator\\Desktop\\hbdt_focus_platform.dat") as hbdt_focus_data:
        for hbdt_data in hbdt_focus_data:
            try:
                (userid, phoneid, phone, token, flyid, focusdate, flydate
                 , createtime, platform, ordertype) = hbdt_data.strip().split("\t")
            except Exception:
                continue

            phone_id = str(userid) + '_' + str(phoneid)
            query_date = createtime.split(" ")[0] if createtime.split(" ")[0] != "None" else focusdate.split(" ")[0]
            if query_date == "None":
                continue
            # insert_bit(phone_id)
            f.add(phone_id)
    with open("./bloom.txt", "wb") as bloom_f:
        f.tofile(bloom_f)


def hash_fun(val):
    return mmh3.hash(val, 41) % bit_size


def init_bit():
    pass


def insert_bit(val):
    bit_location = hash_fun(val)
    # pipe = DBCli().redis_dt_cli.pipeline()
    # for loc in bit_location:
    #     pipe.setbit("bit_focus_his_uid", loc, 1)
    # pipe.execute()
    bit_location = hash_fun(val)
    DBCli().redis_dt_cli.setbit("bit_focus_his_uid", bit_location, 1)


def is_contains(val):
    bit_location = hash_fun(val)
    # 把要比较的值通过k各hash函数hash到不同的bit位置上, 只有一个位置为0那么就不存在
    # return all(True if DBCli().redis_dt_cli.getbit("bit_focus_his_uid", loc) else False for loc in bit_location)
    return DBCli().redis_dt_cli.getbit("bit_focus_his_uid", bit_location)


def test1():
    f = open("C:\\Users\\Administrator\\Documents\\access.log_20160905", 'r')
    for i in f:
        pass
    f.close()


def test2():
    f = open("C:\\Users\\Administrator\\Documents\\access.log_20160905", 'r')
    for i in f.xreadlines():
        pass
    f.close()

if __name__ == "__main__":
    import tornado.httpserver
    import tornado.ioloop
    import tornado.options
    import tornado.web
    import tornado.httpclient
    from tornado import gen
    from sql.huoli_sqlHandlers import car_consumers_sql
    from tornado.options import define, options
    from tornado.concurrent import run_on_executor
    from concurrent.futures import ThreadPoolExecutor

    define("port", default=8000, help="run on the given port", type=int)


    class SleepHandler(tornado.web.RequestHandler):
        executor = ThreadPoolExecutor(1)

        @run_on_executor
        def testApp(self):
            today = DateUtil.get_date_before_days(int(1))
            dto = []
            for i in xrange(3):
                dto.append(DateUtil.date2str(today, '%Y-%m-%d'))
                dto.append(DateUtil.date2str(today, '%Y-%m-%d'))
                dto.append(DateUtil.date2str(today, '%Y-%m-%d'))
            result = DBCli().car_cli.query_one(car_consumers_sql['car_newconsumers_daily'], dto)
            return result

        @tornado.gen.coroutine
        def get(self):
            a = yield self.testApp()
            self.write('haha')

    class JustNowHandler(tornado.web.RequestHandler):
        def get(self):
            self.write("i hope just now see you")


    tornado.options.parse_command_line()
    app = tornado.web.Application(handlers=[
        (r"/sleep", SleepHandler), (r"/justnow", JustNowHandler)])
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    print 'server is running'
    tornado.ioloop.IOLoop.instance().start()

