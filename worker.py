# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import mmh3

SEEDS = [53, 59, 61, 67, 71, 73, 79, 83]
bit_size = 1 << 32


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
    focus_newuser = []
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
            if not is_contains(phone_id):
                focus_newuser.append(phone_id)
    focus_pv = len(query_id)
    focus_uv = len(set(query_id))
    print focus_uv, focus_pv, len(focus_newuser)


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
            insert_bit(phone_id)


def hash_fun(val):
    return [mmh3.hash(val, seed) % bit_size for seed in SEEDS]


def init_bit():
    pass


def insert_bit(val):
    bit_location = hash_fun(val)
    pipe = DBCli().redis_dt_cli.pipeline()
    for loc in bit_location:
        pipe.setbit("bit_focus_his_uid", loc, 1)
    pipe.execute()


def is_contains(val):
    bit_location = hash_fun(val)
    # 把要比较的值通过k各hash函数hash到不同的bit位置上, 只有一个位置为0那么就不存在
    return all(True if DBCli().redis_dt_cli.getbit("bit_focus_his_uid", loc) else False for loc in bit_location)


if __name__ == "__main__":
    # 计算redis原始比较的
    import time
    start = time.time()
    # collect_his_phone_uid()
    # update_focus_newuser(1)
    print time.time() - start

    print "========================="
    start = time.time()
    collect_his_phone_uid_bit()
    update_focus_newuser_bit(1)
    print time.time() - start
    print hash_fun("35400638_3869643")