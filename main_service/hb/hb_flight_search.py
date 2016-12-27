#coding:utf8
from sql.hb_sqlHandlers import hb_flight_search_user_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from dbClient import utils
import json
import requests
import datetime
import time


def localytics_cli(app_id, event, metrics, start_date):
    # api_key = "dd633143c1a14867726b60a-812924b6-5b0b-11e6-71ff-002dea3c3994"
    # api_secret = "f91925eb1865c8431589ff2-81292808-5b0b-11e6-71ff-002dea3c3994"
    api_key = "0d2eb34de63f71462c15f0e-3f4088c2-5f00-11e6-7216-002dea3c3994"
    api_secret = "2049d2d0815af8273eff9e4-3f408e30-5f00-11e6-7216-002dea3c3994"
    api_root = "https://api.localytics.com/v1/query"
    # app_id = "2c64c068203c5033ddb127f-c76c5cc2-582a-11e5-07bf-00deb82fd81f"
    data_params = {"app_id": app_id, "dimensions": "day", "metrics": metrics}

    conditions = {"event_name": event, "day": ["between", start_date, start_date]}

    data_params["conditions"] = json.dumps(conditions)

    r = requests.get(api_root, auth=(api_key, api_secret), params=data_params)
    result = r.json()
    return (result["results"])[0][metrics]


def update_flight_search_user_daily(days=0):
    tomorrow = DateUtil.getDateAfterDays(1-int(days))
    tomorrow_date = DateUtil.date2str(tomorrow)
    today = DateUtil.getDateBeforeDays(int(days))
    s_day = DateUtil.date2str(DateUtil.getDateBeforeDays(int(days)), '%Y-%m-%d')
    tablename = DateUtil.getTable(today)
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
            localytics_result = localytics_cli(app_id[x], event_list[x], i, s_day)
            localytics_check[i] += localytics_result
    query_data.append(localytics_check["users"])
    query_data.append(localytics_check["sessions"])

    DBCli().targetdb_cli.insert(hb_flight_search_user_sql['update_flight_search_user_daily'], query_data)


def update_flight_search_user_weekly():
    start_week, end_week = DateUtil.getLastWeekDate()
    end_table = DateUtil.getTable(DateUtil.add_days(end_week, -1))
    start_table = DateUtil.getTable(start_week)
    start_week = DateUtil.date2str(start_week)
    end_week = DateUtil.date2str(end_week)
    print start_week, end_week
    if end_table != start_table:
        dto = [start_week, end_week, start_week, end_week, start_week,
               start_table, end_table]
        sql = hb_flight_search_user_sql['hb_filght_search_user_table_weekly']
    else:
        dto = [start_week, end_week, start_week, end_table]
        sql = hb_flight_search_user_sql['hb_filght_search_user_weekly']
    query_data = DBCli().Apilog_cli.queryOne(sql, dto)
    DBCli().targetdb_cli.insert(hb_flight_search_user_sql['update_flight_search_user_weekly'], query_data)


def update_flight_search_user_monthly():
    last_month_start, last_month_end = DateUtil.getLastMonthDate()
    table_list = DateUtil.getAllTable(last_month_start.year, last_month_start.month)
    dto = [last_month_start]
    for i in table_list:
        dto.append(i)
    query_date = DBCli().Apilog_cli.queryOne(hb_flight_search_user_sql['hb_filght_search_user_monthly'], dto)
    DBCli().targetdb_cli.insert(hb_flight_search_user_sql['update_flight_search_user_monthly'], query_date)


def update_flight_search_user_quarterly():
    last_month_start, last_month_end = DateUtil.getLastQuarterDate()
    dto = [last_month_start, last_month_start]
    start_index = last_month_start.month
    if last_month_end.month < start_index:
        end_index = 13
    else:
        end_index = last_month_end.month
    for tablelist in xrange(start_index, end_index):
        table_list = DateUtil.getAllTable(last_month_start.year, tablelist)
        dto.extend(table_list)
    query_date = DBCli().Apilog_cli.queryOne(hb_flight_search_user_sql['hb_filght_search_user_quarterly'], dto)
    DBCli().targetdb_cli.insert(hb_flight_search_user_sql['update_flight_search_user_quarterly'], query_date)


def update_check_pv_his(start_date=(datetime.date(2016, 5, 31))):
    # s_day = DateUtil.date2str(DateUtil.getDateBeforeDays(int(days)), '%Y-%m-%d')
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
        print start_date
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
        print insert_data
        DBCli().targetdb_cli.insert(update_pv_check_sql, insert_data)
        start_date = DateUtil.add_days(start_date, -1)

if __name__ == "__main__":
    # for x in xrange(6, 0, -1):
    # start_date = datetime.date(2016, 1, 31)
    # update_flight_search_user_daily(1)
    # s = dict()
    # s.update()
    # update_check_pv_his(start_date)
    update_flight_search_user_weekly()
    # update_flight_search_user_monthly()