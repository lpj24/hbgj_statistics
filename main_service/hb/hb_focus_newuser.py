# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_focus_newuser(days=0):
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    query_file = start_date + "_hbdt_focus.dat"
    insert_sql = """
        insert into hbdt_focus_newusers_daily (s_day, uv, pv, new_users, createtime, updatetime)
        values (%s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now() ,
        s_day = values(s_day),
        uv = values(uv),
        pv = values(pv),
        new_users = values(new_users)
    """
    query_id = list()
    with open("/home/huolibi/data/hbdt/hbdt_focus/" + query_file) as hbdt_focus_data:
        for hbdt_data in hbdt_focus_data:
            try:
                (userid, phoneid, phone, token, flyid, focusdate, flydate
                 , createtime, platform, ordertype) = hbdt_data.strip().split("\t")
            except Exception:
                continue

            phone_id = str(userid) + '_' + str(phoneid)
            query_date = focusdate.split(" ")[0] if focusdate.split(" ")[0] != "None" else createtime.split(" ")[0]
            if query_date == "None":
                continue
            query_id.append(phone_id)
    focus_pv = len(query_id)
    focus_uv = len(set(query_id))
    his_focus_id = DBCli().redis_dt_cli.smembers("hbdt_focus_his_uid")
    focus_newuser = len(set(query_id).difference(his_focus_id))

    DBCli().targetdb_cli.insert(insert_sql, [start_date, focus_uv, focus_pv, focus_newuser])
    for focus_id in query_id:
        DBCli().redis_dt_cli.sadd("hbdt_focus_his_uid", focus_id)


def collect_his_phone_uid():
    with open("/home/huolibi/data/hbdt/hbdt_focus/hbdt_platform_his.dat") as hbdt_focus_data:
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

if __name__ == "__main__":
    # collect_his_phone_uid()
    update_focus_newuser(1)
    # for x in xrange(22, 0, -1):
    #     update_focus_newuser(x)

