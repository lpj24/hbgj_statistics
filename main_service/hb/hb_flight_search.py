# -*- coding: utf-8 -*-
from sql.hb_sqlHandlers import hb_flight_search_user_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from dbClient import utils
import json
import requests
import datetime
import time


def localytics_cli(app_id, event, metrics, start_date):
    api_key = "0d2eb34de63f71462c15f0e-3f4088c2-5f00-11e6-7216-002dea3c3994"
    api_secret = "2049d2d0815af8273eff9e4-3f408e30-5f00-11e6-7216-002dea3c3994"
    api_root = "https://api.localytics.com/v1/query"
    # app_id = "2c64c068203c5033ddb127f-c76c5cc2-582a-11e5-07bf-00deb82fd81f"
    data_params = {"app_id": app_id, "dimensions": "day", "metrics": metrics}

    conditions = {"event_name": event, "day": ["between", start_date, start_date]}

    data_params["conditions"] = json.dumps(conditions)

    try:
        r = requests.get(api_root, auth=(api_key, api_secret), params=data_params, timeout=60)
    except Exception:
        raise RuntimeError("localytics time out")
    result = r.json()
    status_code = r.status_code
    return status_code, (result["results"])[0][metrics]


def update_flight_search_user_daily(days=0):
    """更新航班搜索pv与uv, hbdt_search_daily"""
    tomorrow = DateUtil.get_date_after_days(1-int(days))
    tomorrow_date = DateUtil.date2str(tomorrow)
    today = DateUtil.get_date_before_days(int(days))
    s_day = DateUtil.date2str(DateUtil.get_date_before_days(int(days)), '%Y-%m-%d')
    # tablename = DateUtil.get_table(today)
    tablename = "flightApiLog_" + DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y%m%d')
    dto = [DateUtil.date2str(today, '%Y-%m-%d'), DateUtil.date2str(today), tomorrow_date, tablename]
    pv_check_dto = [DateUtil.date2str(today, '%Y-%m-%d'), ]
    pv_check_sql = """
        select pv from (
            select time,sum(access) pv
            from ADVICE_INFO
            where id like '4313%%' or id like '4312%%'
            group by time
            order by time) tmp
            where tmp.time = %s
    """

    pv_check_data = DBCli().sourcedb_cli.queryOne(pv_check_sql, pv_check_dto)
    pv_check = pv_check_data[0]
    query_data = DBCli().Apilog_cli.queryOne(hb_flight_search_user_sql['hb_filght_search_user_daily'], dto)
    pv = query_data[2]
    if pv > 0:
        if float(int(pv_check) - int(pv)) / float(pv) > 0.2:
            utils.sendMail("lipenju24@163.com", str(pv_check) + ":" + str(pv), "航班搜索数据错误")
            query_data = [query_data[0], query_data[1], int(pv)]

    query_data = list(query_data)
    query_data.append(int(pv_check))

    app_id_android = "2c64c068203c5033ddb127f-c76c5cc2-582a-11e5-07bf-00deb82fd81f"
    app_id_ios = "c0b8588071fc960755ee311-9ac01816-582a-11e5-ba3c-0013a62af900"

    app_id = [app_id_android, app_id_ios]
    event_list = ["android.status.query.open", "ios.status.query.open"]
    metrics = ["sessions", "users"]

    localytics_check = {"sessions": 0, "users": 0}
    for i in metrics:
        for x in xrange(len(metrics)):
            status_code, localytics_result = localytics_cli(app_id[x], event_list[x], i, s_day)
            if status_code == 429:
                raise AssertionError("localytics over times")
                return
            localytics_check[i] += localytics_result
    query_data.append(localytics_check["users"])
    query_data.append(localytics_check["sessions"])
    DBCli().targetdb_cli.insert(hb_flight_search_user_sql['update_flight_search_user_daily'], query_data)
    return __file__


def update_dt_search_uid(days=0):
    """更新航班搜索uid计算周月数据, redis数据库"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - days), '%Y-%m-%d')
    sql = """
        SELECT distinct uid FROM tablename where logTime>=%s
        and logTime<%s
        and (pid = '4312' or pid = '4313')
    """

    tablename = "flightApiLog_" + DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y%m%d')
    dto = [start_date, end_date, tablename]
    uids = DBCli().Apilog_cli.queryAll(sql, dto)
    for uid in uids:
        DBCli().redis_dt_cli.sadd(start_date + "_hbdt_search", uid[0])


def update_flight_search_user_weekly():
    start_week, end_week = DateUtil.get_last_week_date()
    s_day = DateUtil.date2str(start_week, '%Y-%m-%d')
    dto = [start_week, end_week]
    query_key = []
    while end_week > start_week:
        query_key.append(DateUtil.date2str(start_week, '%Y-%m-%d') + "_hbdt_search")
        start_week = DateUtil.add_days(start_week, 1)
    week_uv = len(DBCli().redis_dt_cli.sunion(query_key))

    pv_sql = """
            select pv from hbdt_search_daily where s_day>=%s and s_day<%s
        """

    pv_data = DBCli().targetdb_cli.queryAll(pv_sql, dto)
    pv_sum = 0
    for pv in pv_data:
        pv_sum += pv[0]
    DBCli().targetdb_cli.insert(hb_flight_search_user_sql['update_flight_search_user_weekly'], [s_day, week_uv, pv_sum])
    # start_week, end_week = DateUtil.get_last_week_date()
    # end_table = DateUtil.get_table(DateUtil.add_days(end_week, -1))
    # start_table = DateUtil.get_table(start_week)
    # start_week = DateUtil.date2str(start_week)
    # end_week = DateUtil.date2str(end_week)
    # if end_table != start_table:
    #     dto = [start_week, end_week, start_week, end_week, start_week,
    #            start_table, end_table]
    #     sql = hb_flight_search_user_sql['hb_filght_search_user_table_weekly']
    # else:
    #     dto = [start_week, end_week, start_week, end_table]
    #     sql = hb_flight_search_user_sql['hb_filght_search_user_weekly']
    # query_data = DBCli().Apilog_cli.queryOne(sql, dto)
    # DBCli().targetdb_cli.insert(hb_flight_search_user_sql['update_flight_search_user_weekly'], query_data)


def update_flight_search_user_monthly():
    start_month, end_month = DateUtil.get_last_month_date()
    s_day = DateUtil.date2str(start_month, '%Y-%m-%d')
    dto = [start_month, end_month]
    query_key = []
    while end_month > start_month:
        query_key.append(DateUtil.date2str(start_month, '%Y-%m-%d') + "_hbdt_search")
        start_month = DateUtil.add_days(start_month, 1)
    month_uv = len(DBCli().redis_dt_cli.sunion(query_key))

    pv_sql = """
            select pv from hbdt_search_daily where s_day>=%s and s_day<%s
        """

    pv_data = DBCli().targetdb_cli.queryAll(pv_sql, dto)
    pv_sum = 0
    for pv in pv_data:
        pv_sum += pv[0]
    DBCli().targetdb_cli.insert(hb_flight_search_user_sql['update_flight_search_user_monthly'], [s_day, month_uv, pv_sum])

    # last_month_start, last_month_end = DateUtil.get_last_month_date()
    # table_list = DateUtil.get_all_table(last_month_start.year, last_month_start.month)
    # dto = [last_month_start]
    # for i in table_list:
    #     dto.append(i)
    # query_data = DBCli().Apilog_cli.queryOne(hb_flight_search_user_sql['hb_filght_search_user_monthly'], dto)
    # DBCli().targetdb_cli.insert(hb_flight_search_user_sql['update_flight_search_user_monthly'], query_data)


def update_flight_search_user_quarterly():
    start_quarter, end_quarter = DateUtil.get_last_quarter_date()
    s_day = DateUtil.date2str(start_quarter, '%Y-%m-%d')
    dto = [start_quarter, end_quarter]
    query_key = []
    while end_quarter > start_quarter:
        query_key.append(DateUtil.date2str(start_quarter, '%Y-%m-%d') + "_hbdt_search")
        start_quarter = DateUtil.add_days(start_quarter, 1)
    quarter_uv = len(DBCli().redis_dt_cli.sunion(query_key))

    pv_sql = """
            select pv from hbdt_search_daily where s_day>=%s and s_day<%s
        """

    pv_data = DBCli().targetdb_cli.queryAll(pv_sql, dto)
    pv_sum = 0
    for pv in pv_data:
        pv_sum += pv[0]
    DBCli().targetdb_cli.insert(hb_flight_search_user_sql['update_flight_search_user_quarterly'],
                                [s_day, quarter_uv, pv_sum])


def update_check_pv_his(start_date=(datetime.date(2016, 5, 31))):
    # s_day = DateUtil.date2str(DateUtil.get_date_before_days(int(days)), '%Y-%m-%d')
    pv_check_sql = """
        select pv from (
            select time,sum(access) pv
            from ADVICE_INFO
            where id like '4313%%' or id like '4312%%'
            group by time
            order by time) tmp
            where tmp.time = %s
    """

    update_pv_check_sql = """
            insert into hbdt_search_daily (s_day, localytics_uv, localytics_pv,
            createtime, updatetime) values (%s, %s, %s, now(), now())
            on duplicate key update updatetime = now() ,
            s_day = VALUES(s_day),
            localytics_uv = VALUES(localytics_uv),
            localytics_pv = VALUES(localytics_pv)

    """
    # start_date = datetime.date(2016, 5, 31)
    end_date = datetime.date(2013, 1, 1)

    while start_date >= end_date:
        insert_data = []
        s_day = DateUtil.date2str(start_date, '%Y-%m-%d')
        pv_check_dto = [str(s_day), ]
        # query_data = DBCli().sourcedb_cli.queryOne(pv_check_sql, pv_check_dto)

        app_id_android = "2c64c068203c5033ddb127f-c76c5cc2-582a-11e5-07bf-00deb82fd81f"
        app_id_ios = "c0b8588071fc960755ee311-9ac01816-582a-11e5-ba3c-0013a62af900"

        app_id = [app_id_android, app_id_ios]
        event_list = ["android.status.query.open", "ios.status.query.open"]
        metrics = ["sessions", "users"]

        insert_data.append(s_day)
        # insert_data.append(query_data[0])
        localytics_check = {"sessions": 0, "users": 0}
        for i in metrics:
            for x in xrange(len(metrics)):
                try:
                    localytics_result = localytics_cli(app_id[x], event_list[x], i, s_day)
                    localytics_check[i] += localytics_result
                except:
                    time.sleep(60 * 60)
                    update_check_pv_his(start_date)
        insert_data.append(localytics_check["users"])
        insert_data.append(localytics_check["sessions"])
        DBCli().targetdb_cli.insert(update_pv_check_sql, insert_data)
        start_date = DateUtil.add_days(start_date, -1)

if __name__ == "__main__":
    # for x in xrange(6, 0, -1):
    # start_date = datetime.date(2016, 1, 31)
    # i = 6
    # while i >= 1:
    #     update_dt_search_uid(i)
    #     i -= 1
    # update_flight_search_user_daily(3)
    # update_flight_search_user_daily(41)
    # i = 25
    # while i >= 19:
    #     update_flight_search_user_daily(i)
    #     i -= 1
    # s = dict()
    # s.update()
    # update_check_pv_his(start_date)
    # update_flight_search_user_weekly()
    # update_flight_search_user_monthly()
    update_flight_search_user_daily(1)
