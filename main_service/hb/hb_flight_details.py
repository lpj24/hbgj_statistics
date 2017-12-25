# -*- coding: utf-8 -*-
from sql.hb_sqlHandlers import hb_flight_detail_user_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from dbClient import utils
import requests
import json
import time
import datetime
from collections import defaultdict, Counter


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


# def update_flight_detail_user_daily(days=0):
#     """更新航班详情pv与uv, hbdt_details_daily"""
#     tomorrow = DateUtil.get_date_after_days(1-int(days))
#     tomorrow_date = DateUtil.date2str(tomorrow)
#     today = DateUtil.date2str(DateUtil.get_date_before_days(int(days)))
#     s_day = DateUtil.date2str(DateUtil.get_date_before_days(int(days)), '%Y-%m-%d')
#     # tablename = DateUtil.get_table(DateUtil.get_date_before_days(int(days)))
#
#     tablename = "flightApiLog_" + DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y%m%d')
#     dto = [s_day, today, tomorrow_date, tablename]
#     pv_check_dto = [str(s_day), ]
#     pv_check_sql = """
#         select pv from (
#             select time,sum(access) pv
#             from ADVICE_INFO
#             where id like '4314%%'
#             group by time
#             order by time) tmp
#             where tmp.time = %s
#     """
#
#     pv_check_data = DBCli().sourcedb_cli.query_one(pv_check_sql, pv_check_dto)
#     pv_check = pv_check_data[0]
#     query_data = DBCli().Apilog_cli.query_one(hb_flight_detail_user_sql['hb_filght_detail_user_daily'], dto)
#     pv = query_data[2]
#     if int(pv) > 0:
#         if float(int(pv_check) - int(pv))/float(pv) > 0.2:
#             # utils.sendMail("lipenju24@163.com", s_day + str(pv_check) + ":" + str(pv), "航班动态数据错误")
#             query_data = [query_data[0], query_data[1], int(pv)]
#     query_data = list(query_data)
#     query_data.append(int(pv_check))
#
#     app_id_android = "2c64c068203c5033ddb127f-c76c5cc2-582a-11e5-07bf-00deb82fd81f"
#     app_id_ios = "c0b8588071fc960755ee311-9ac01816-582a-11e5-ba3c-0013a62af900"
#
#     app_id = [app_id_android, app_id_ios]
#     event_list = ["android.status.detail.open", "ios.status.detail.open"]
#     metrics = ["sessions", "users"]
#
#     localytics_check = {"sessions": 0, "users": 0}
#     for i in metrics:
#         for x in xrange(len(metrics)):
#             status_code, localytics_result = localytics_cli(app_id[x], event_list[x], i, s_day)
#             if status_code == 429:
#                 raise AssertionError("localytics over times")
#                 return
#             localytics_check[i] += localytics_result
#     query_data.append(localytics_check["users"])
#     query_data.append(localytics_check["sessions"])
#     DBCli().targetdb_cli.insert(hb_flight_detail_user_sql['update_flight_detail_user_daily'], query_data)


def update_dt_detail_uid(days=0):
    """更新航班动态uid用于计算周数据与月, redis数据库"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - days), '%Y-%m-%d')
    sql = """
        SELECT distinct uid FROM tablename where logTime>=%s
        and logTime<%s
        and pid='4314'
    """

    tablename = "flightApiLog_" + DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y%m%d')
    dto = [start_date, end_date, tablename]
    uids = DBCli().Apilog_cli.query_all(sql, dto)
    for uid in uids:
        DBCli().redis_dt_cli.sadd(start_date + "_hbdt_detail", uid[0])


def update_flight_detail_user_weekly():
    """更新航班动态详情用户周, hbdt_details_weekly"""
    start_week, end_week = DateUtil.get_last_week_date()
    s_day = DateUtil.date2str(start_week, '%Y-%m-%d')
    dto = [start_week, end_week]
    query_key = []
    while end_week > start_week:
        query_key.append(DateUtil.date2str(start_week, '%Y-%m-%d') + "_hbdt_detail")
        start_week = DateUtil.add_days(start_week, 1)
    week_uv = len(DBCli().redis_dt_cli.sunion(query_key))

    pv_sql = """
        select pv from hbdt_details_daily where s_day>=%s and s_day<%s
    """

    pv_data = DBCli().targetdb_cli.query_all(pv_sql, dto)
    pv_sum = 0
    for pv in pv_data:
        pv_sum += pv[0]
    DBCli().targetdb_cli.insert(hb_flight_detail_user_sql['update_flight_detail_user_weekly'], [s_day, week_uv, pv_sum])
    # end_table = DateUtil.get_table(DateUtil.add_days(end_week, -1))
    # start_table = DateUtil.get_table(start_week)
    # start_week = DateUtil.date2str(start_week)
    # end_week = DateUtil.date2str(end_week)
    # if end_table != start_table:
    #     dto = [start_week, end_week, start_week, end_week, start_week,
    #            start_table, end_table]
    #     sql = hb_flight_detail_user_sql['hb_filght_detail_user_table_weekly']
    # else:
    #     dto = [start_week, end_week, start_week, end_table]
    #     sql = hb_flight_detail_user_sql['hb_filght_detail_user_weekly']
    # query_data = DBCli().Apilog_cli.query_one(sql, dto)
    # DBCli().targetdb_cli.insert(hb_flight_detail_user_sql['update_flight_detail_user_weekly'], query_data)


def update_flight_detail_user_monthly():
    start_month, end_month = DateUtil.get_last_month_date()
    s_day = DateUtil.date2str(start_month, '%Y-%m-%d')
    dto = [start_month, end_month]
    query_key = []
    while end_month > start_month:
        query_key.append(DateUtil.date2str(start_month, '%Y-%m-%d') + "_hbdt_detail")
        start_month = DateUtil.add_days(start_month, 1)
    month_uv = len(DBCli().redis_dt_cli.sunion(query_key))

    pv_sql = """
        select pv from hbdt_details_daily where s_day>=%s and s_day<%s
    """

    pv_data = DBCli().targetdb_cli.query_all(pv_sql, dto)
    pv_sum = 0
    for pv in pv_data:
        pv_sum += pv[0]
    DBCli().targetdb_cli.insert(hb_flight_detail_user_sql['update_flight_detail_user_monthly'], [s_day, month_uv, pv_sum])
    # last_month_start, last_month_end = DateUtil.get_last_month_date()
    # table_list = DateUtil.get_all_table(last_month_start.year, last_month_start.month)
    # dto = [last_month_start]
    # for i in table_list:
    #     dto.append(i)
    # query_data = DBCli().Apilog_cli.query_one(hb_flight_detail_user_sql['hb_filght_detail_user_monthly'], dto)
    # DBCli().targetdb_cli.insert(hb_flight_detail_user_sql['update_flight_detail_user_monthly'], query_data)


def update_flight_detail_user_quarterly():
    start_quarter, end_quarter = DateUtil.get_last_quarter_date()
    s_day = DateUtil.date2str(start_quarter, '%Y-%m-%d')
    dto = [start_quarter, end_quarter]
    query_key = []
    while end_quarter > start_quarter:
        query_key.append(DateUtil.date2str(start_quarter, '%Y-%m-%d') + "_hbdt_detail")
        start_quarter = DateUtil.add_days(start_quarter, 1)
    quarter_uv = len(DBCli().redis_dt_cli.sunion(query_key))

    pv_sql = """
            select pv from hbdt_details_daily where s_day>=%s and s_day<%s
        """

    pv_data = DBCli().targetdb_cli.query_all(pv_sql, dto)
    pv_sum = 0
    for pv in pv_data:
        pv_sum += pv[0]
    DBCli().targetdb_cli.insert(hb_flight_detail_user_sql['update_flight_detail_user_quarterly'],
                                [s_day, quarter_uv, pv_sum])


def update_check_pv_his(start_date=(datetime.date(2016, 3, 8))):
    # s_day = DateUtil.date2str(DateUtil.get_date_before_days(int(days)), '%Y-%m-%d')
    pv_check_sql = """
        select pv from (
            select time,sum(access) pv
            from ADVICE_INFO
            where id like '4314%%'
            group by time
            order by time) tmp
            where tmp.time = %s
    """

    update_pv_check_sql = """
            insert into hbdt_details_daily (s_day, localytics_uv, localytics_pv,
            createtime, updatetime) values (%s, %s, %s, now(), now())
            on duplicate key update updatetime = now() ,
            s_day = VALUES(s_day),
            localytics_uv = VALUES(localytics_uv),
            localytics_pv = VALUES(localytics_pv)

    """

    end_date = datetime.date(2013, 1, 1)

    while start_date >= end_date:
        insert_data = []
        s_day = DateUtil.date2str(start_date, '%Y-%m-%d')
        pv_check_dto = [str(s_day), ]
        query_data = DBCli().sourcedb_cli.query_one(pv_check_sql, pv_check_dto)

        app_id_android = "2c64c068203c5033ddb127f-c76c5cc2-582a-11e5-07bf-00deb82fd81f"
        app_id_ios = "c0b8588071fc960755ee311-9ac01816-582a-11e5-ba3c-0013a62af900"

        app_id = [app_id_android, app_id_ios]
        event_list = ["android.status.detail.open", "ios.status.detail.open"]
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
                    time.sleep(60*60)
                    update_check_pv_his(start_date)
        insert_data.append(localytics_check["users"])
        insert_data.append(localytics_check["sessions"])
        DBCli().targetdb_cli.insert(update_pv_check_sql, insert_data)
        start_date = DateUtil.add_days(start_date, -1)


def update_hb_city_rate(days=0):
    """更新航班准点率 延误率 取消率, hbgj_flightdyn_company_daily hbgj_flightdyn_depcity_daily"""
    import os
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 2), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - days), '%Y-%m-%d')
    hb_sql = """
        SELECT to_char(to_date(DTFS_FLIGHTDEPTIMEPLAN, 'yyyy-mm-dd hh24:mi:ss'), 'yyyy-mm-dd') s_day,
        DTFS_FLIGHTCOMPANY,DTFS_FLIGHTNO,DTFS_FLIGHTDEPCODE,DTFS_FLIGHTARRCODE,
        DTFS_FLIGHTDEP,DTFS_FLIGHTARR,dtfs_flightdeptimeplan, DTFS_FLIGHTDEPTIME,DTFS_FLIGHTSTATE,
        (case when DTFS_FLIGHTSTATE='取消' then 1 else 0 end) state_code
        from DAY_FLY_DTINFO
        where DTFS_MARK = 0 AND DTFS_SHARE = 0 AND DTFS_STOP = '0'
        AND DTFS_FLIGHTDEPTIMEPLAN>=:start_date
        AND DTFS_FLIGHTDEPTIMEPLAN<:end_date
    """
    city_dict = get_city_code_dict()
    company_dict = get_aircompany_dict()
    query_data = DBCli().flight_oracle_cli.query_all(hb_sql, [start_date, end_date])
    insert_company_data = defaultdict(list)
    insert_city_data = defaultdict(list)
    insert_com_list = []
    insert_company_sql = """
        insert into hbgj_flightdyn_company_daily
        (s_day, company_name, company_code, time_num, delay_num, cancel_num, createtime, updatetime) values
            (%s, %s, %s, %s, %s, %s, now(), now())
            on duplicate key update updatetime = now() ,
            s_day = VALUES(s_day),
            company_name = VALUES(company_name),
            company_code = VALUES(company_code),
            time_num = VALUES(time_num),
            delay_num = VALUES(delay_num),
            cancel_num = VALUES(cancel_num)
    """

    insert_city_list = []

    insert_city_sql = """
        insert into hbgj_flightdyn_depcity_daily (s_day, dep_name, dep_code, time_num,
            delay_num, cancel_num, createtime, updatetime) values
            (%s, %s, %s, %s, %s, %s, now(), now())
            on duplicate key update updatetime = now() ,
            s_day = VALUES(s_day),
            dep_name = VALUES(dep_name),
            dep_code = VALUES(dep_code),
            time_num = VALUES(time_num),
            delay_num = VALUES(delay_num),
            cancel_num = VALUES(cancel_num)
    """
    delete_company_sql = """
        delete from hbgj_flightdyn_company_daily where s_day >= %s and s_day < %s
    """
    delete_dep_sql = """
        delete from hbgj_flightdyn_depcity_daily where s_day >= %s and s_day < %s
    """
    DBCli().targetdb_cli.insert(delete_company_sql, [start_date, end_date])
    DBCli().targetdb_cli.insert(delete_dep_sql, [start_date, end_date])
    for data in query_data:
        s_day, hb_company, fly_no, fly_depcode, fly_arrcode, \
        fly_depcity, fly_arrcity, plan_dep_time, dep_time, fly_state, fly_state_code = data
        try:
            if int(fly_state_code) == 0:
                diff_min = diff_days(dep_time, plan_dep_time)
                if diff_min >= 30:
                    # 延误
                    insert_company_data[s_day + ':' + company_dict[fly_no[:2]] + ':' + fly_no[:2]].append(1)
                    insert_city_data[s_day + ':' + city_dict[fly_depcode] + ':' + fly_depcode].append(1)
                else:
                    # 准点
                    insert_company_data[s_day + ':' + company_dict[fly_no[:2]] + ':' + fly_no[:2]].append(0)
                    insert_city_data[s_day + ':' + city_dict[fly_depcode] + ':' + fly_depcode].append(0)
            elif int(fly_state_code) == 1:
                # 取消
                insert_company_data[s_day + ':' + company_dict[fly_no[:2]] + ':' + fly_no[:2]].append(-1)
                insert_city_data[s_day + ':' + city_dict[fly_depcode] + ':' + fly_depcode].append(-1)
        except (KeyError, ):
            continue
    for k, v in Counter(insert_company_data).items():
        fly_num = Counter(v)
        s_day, company, c_code = k.split(':')
        delay_num = fly_num[1]
        time_num = fly_num[0]
        cancel_num = fly_num[-1]
        insert_com_list.append([s_day, company, c_code, time_num, delay_num, cancel_num])

    for k, v in Counter(insert_city_data).items():
        fly_num = Counter(v)
        s_day, c_city, c_code = k.split(':')
        delay_num = fly_num[1]
        time_num = fly_num[0]
        cancel_num = fly_num[-1]
        insert_city_list.append([s_day, c_city, c_code, time_num, delay_num, cancel_num])

    insert_com_list = sorted(insert_com_list, key=lambda x: x[-1] + x[-2] + x[-3], reverse=True)
    insert_city_list = sorted(insert_city_list, key=lambda x: x[-1] + x[-2] + x[-3], reverse=True)
    DBCli().targetdb_cli.batch_insert(insert_company_sql, insert_com_list)
    DBCli().targetdb_cli.batch_insert(insert_city_sql, insert_city_list)


def diff_days(one_date=None, two_date=None):
    if one_date is None or two_date is None:
        return -1
    dep_date, plan_date = DateUtil.str2date(one_date), DateUtil.str2date(two_date)

    if dep_date >= plan_date:
        return (dep_date - plan_date).seconds/60
    else:
        return -1


def get_aircompany_dict():
    hb_code_sql = """
        select code,FOUR_NAME
        from AIRLINES_NORMAl
    """
    hb_info = DBCli().oracle_cli.query_all(hb_code_sql)
    hb_info = dict(hb_info)
    return hb_info


def get_city_code_dict():
    city_sql = """
        select THREE_WORDS_CODE , CITY_NAME from apibase.AIRPORT_NATION_INFO
    """
    city_dict = DBCli().sourcedb_cli.query_all(city_sql)
    return dict(city_dict)


def update_flight_detail_search_user_daily(days=0):
    """更新航班详情pv与uv, hbdt_details_daily hbdt_saerch_daily"""
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1-int(days)), '%Y-%m-%d')
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    sql = """
        select dt, type, pv , uv from hb_dynamicinfo_day where
        dt >= %s
        and dt < %s
    """
    insert_sql = """
        insert into {0} 
        (s_day, uv, pv, createtime, updatetime)
        values
        (%s, %s, %s, now(), now())
        on duplicate key update updatetime = now() ,
        s_day = VALUES(s_day),
        pv = VALUES(pv),
        uv = VALUES(uv)
    """
    query_data = DBCli().Apilog_cli.query_all(sql, [start_date, end_date])
    print query_data
    for data in query_data:
        dt, s_type, pv, uv = data
        insert_data = [dt, uv, pv]
        if s_type == 'D_Search':
            DBCli().targetdb_cli.insert(insert_sql.format('hbdt_search_daily'), insert_data)
        else:
            DBCli().targetdb_cli.insert(insert_sql.format('hbdt_details_daily'), insert_data)


if __name__ == "__main__":
    # update_dt_detail_uid(1)
    update_flight_detail_search_user_daily(1)
    # i = 33
    # for i in xrange(1, 34):
    #     update_flight_detail_search_user_daily(i)