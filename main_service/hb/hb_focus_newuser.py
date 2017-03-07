# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_focus_newuser(days=0):
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    query_file = start_date + "_hbdt_focus.dat"
    check_s_day = DateUtil.get_date_before_days(days * 2)
    check_last_sql = """
        select count(*) from hbdt_focus_newusers_daily where s_day = %s
    """

    check_data = DBCli().targetdb_cli.queryOne(check_last_sql, [check_s_day])
    if int(check_data[0]) < 1:
        return
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

    with open("/home/huolibi/data/hbdt/hbdt_focus/hbdt_focus_platform_his.dat") as hbdt_focus_data:
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


def update_fouces_dat_daily(days=0):
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - days), '%Y-%m-%d')
    sql = """
        SELECT userid, phoneid, phone, token, flyid, focusdate, FOCUSFLYDATE
        , createtime, platform, ordertype FROM FLY_USERFOCUS_TBL
        where createtime<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
        and createtime>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
        union
        SELECT userid, phoneid, phone, token, flyid, focusdate, FOCUSFLYDATE
        , createtime, platform, ordertype FROM FLY_USERFOCUS_TBL_HIS
        where createtime<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
        and createtime>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
    """
    hbdt_focus_file = start_date + "_hbdt_focus.dat"
    hbdt_focus_file = open("/home/huolibi/data/hbdt/hbdt_focus/" + hbdt_focus_file, 'a')
    dto = {"start_date": start_date, "end_date": end_date}
    query_data = DBCli().oracle_cli.queryAll(sql, dto)
    for q in query_data:
        q_list = [str(d) for d in list(q)]
        hbdt_focus_file.write("\t".join(q_list) + "\n")

    hbdt_focus_file.close()


def update_focus_inland_inter_daily(days=0):
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    check_s_day = DateUtil.get_date_before_days(days * 2)
    check_last_sql = """
        select count(*) from hbdt_focus_users_inland_inter_daily where s_day = %s
    """

    check_data = DBCli().targetdb_cli.queryOne(check_last_sql, [check_s_day])
    if int(check_data[0]) < 1:
        return

    insert_sql = """
        insert into hbdt_focus_users_inland_inter_daily (s_day, focus_users_inland, focus_users_inter, exception_users_inter,
        createtime, updatetime) values (%s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now() ,
        s_day = values(s_day),
        focus_users_inland = values(focus_users_inland),
        focus_users_inter = values(focus_users_inter),
        exception_users_inter = values(exception_users_inter)
    """
    query_file = start_date + "_hbdt_focus.dat"
    with open("/home/huolibi/data/hbdt/hbdt_focus/" + query_file) as hbdt_focus_data:
        for hbdt_data in hbdt_focus_data:
            try:
                (userid, phoneid, phone, token, flyid, focusdate, flydate
                 , createtime, platform, ordertype) = hbdt_data.strip().split("\t")
            except Exception:
                continue
            DBCli().redis_dt_cli.sadd(start_date + "_focus_fly", flyid)
    exception_fly = len(DBCli().redis_dt_cli.sdiff(start_date + "_focus_fly", "focus_inland_fly", "focus_inter_fly"))
    inland_fly = len(DBCli().redis_dt_cli.sinter(start_date + "_focus_fly", "focus_inland_fly"))
    inter_fly = len(DBCli().redis_dt_cli.sinter(start_date + "_focus_fly", "focus_inter_fly"))
    DBCli().targetdb_cli.insert(insert_sql, [start_date, inland_fly, inter_fly, exception_fly])
    DBCli().redis_dt_cli.expire(start_date + "_focus_fly", 86400)


def collect_inland_inter_flyid_his():
    fly_info_sql = """
        select FLYID, FLYDEP ,FLYARR from FLY_FLYINFO_TBL where to_char(CREATETIME) < '2017-03-07 00:00:00'
        UNION ALL
        select FLYID, FLYDEP ,FLYARR from FLY_FLYINFO_TBL_HIS
    """
    inland_code_sql = """
        select THREE_WORDS_CODE from AIRPORT_NATION_INFO
    """
    inland_code = DBCli().oracle_cli.queryAll(inland_code_sql)
    inland_code = [in_code[0] for in_code in inland_code]
    fly_info = DBCli().oracle_cli.queryAll(fly_info_sql)
    for fly in fly_info:
        flyid, depcode, arrcode = fly
        if depcode in inland_code or arrcode in inland_code:
            DBCli().redis_dt_cli.sadd("focus_inland_fly", flyid)
        else:
            DBCli().redis_dt_cli.sadd("focus_inter_fly", flyid)


def collect_inland_inter_flyid_daily(days=0):
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days))
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - days))
    dto = {"start_date": start_date, "end_date": end_date}
    fly_info_sql = """
        select FLYID, FLYDEP ,FLYARR from FLY_FLYINFO_TBL where
        CREATETIME < to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
        and
        CREATETIME >= to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
    """
    inland_code_sql = """
        select THREE_WORDS_CODE from AIRPORT_NATION_INFO
    """
    inland_code = DBCli().oracle_cli.queryAll(inland_code_sql)
    inland_code = [in_code[0] for in_code in inland_code]
    fly_info = DBCli().oracle_cli.queryAll(fly_info_sql, dto)
    for fly in fly_info:
        flyid, depcode, arrcode = fly
        if depcode in inland_code or arrcode in inland_code:
            DBCli().redis_dt_cli.sadd("focus_inland_fly", flyid)
        else:
            DBCli().redis_dt_cli.sadd("focus_inter_fly", flyid)


if __name__ == "__main__":
    # collect_inland_inter_flyid_his()
    # collect_inland_inter_flyid_daily(2)
    # collect_his_phone_uid()
    # update_focus_newuser()
    # update_fouces_dat_daily(2)
    for x in xrange(34, 0, -1):
        update_focus_inland_inter_daily(x)
    # collect_inland_inter_flyid_daily(1)

