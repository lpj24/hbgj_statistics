# -*- coding: utf-8 -*-
import requests
import json
from collections import defaultdict
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import datetime
import time


def hb_ticket_book(days=0):
    """localytics机票预定事件(日), ticket_book_event"""
    api_key = "dd633143c1a14867726b60a-812924b6-5b0b-11e6-71ff-002dea3c3994"
    api_secret = "f91925eb1865c8431589ff2-81292808-5b0b-11e6-71ff-002dea3c3994"

    api_root = "https://api.localytics.com/v1/query"
    # app_id = "2c64c068203c5033ddb127f-c76c5cc2-582a-11e5-07bf-00deb82fd81f"
    app_id_android = "2c64c068203c5033ddb127f-c76c5cc2-582a-11e5-07bf-00deb82fd81f"
    app_id_ios = "c0b8588071fc960755ee311-9ac01816-582a-11e5-ba3c-0013a62af900"
    event_list = ["ticket.open", "ticket.query", "ticket.list.detail.click", "ticket.detail.order.online",
                  "ticket.order.pay.start", "ticket.order.pay.succ", "open"]
    hbdt_event = []
    pv_uv = ["sessions_per_event", "users"]

    # start_date = str(datetime.date(2016, 1, 1))
    # end_date = DateUtil.date2str(DateUtil.get_date_before_days(1), '%Y-%m-%d')
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    for event in event_list:
        hbdt_event.append("ios." + event)
        hbdt_event.append("android." + event)

    insert_data = defaultdict(list)

    sql = """
        insert into ticket_book_event (
        s_day,
        ios_ticket_open_pv,
        ios_ticket_open_uv,
        android_ticket_open_pv,
        android_ticket_open_uv,
        ios_ticket_query_pv,
        ios_ticket_query_uv,
        android_ticket_query_pv,
        android_ticket_query_uv,
        ios_ticket_list_detail_click_pv,
        ios_ticket_list_detail_click_uv,
        android_ticket_list_detail_click_pv,
        android_ticket_list_detail_click_uv,
        ios_ticket_detail_order_online_pv,
        ios_ticket_detail_order_online_uv,
        android_ticket_detail_order_online_pv,
        android_ticket_detail_order_online_uv,
        ios_ticket_order_pay_start_pv,
        ios_ticket_order_pay_start_uv,
        android_ticket_order_pay_start_pv,
        android_ticket_order_pay_start_uv,
        ios_ticket_order_pay_succ_pv,
        ios_ticket_order_pay_succ_uv,
        android_ticket_order_pay_succ_pv,
        android_ticket_order_pay_succ_uv,
        ios_open_pv,
        ios_open_uv,
        android_open_pv,
        android_open_uv
        ) values (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    for event in hbdt_event:
        if event.startswith("ios"):
            app_id = app_id_ios
        elif event.startswith("android"):
            app_id = app_id_android

        for dim in pv_uv:

            data_params = {"app_id": app_id, "dimensions": "day", "metrics": dim}

            conditions = {"event_name": event, "day": ["between", start_date, start_date]}

            data_params["conditions"] = json.dumps(conditions)
            try:
                r = requests.get(api_root, auth=(api_key, api_secret), params=data_params, timeout=240)
                result = r.json()
                if r.status_code == 429:
                    raise AssertionError
                data = result["results"]
            except Exception:
                raise AssertionError
                # time.sleep(60*30)
                # hb_ticket_book(1)

            for d in data:
                insert_data[d["day"]].append(d[dim])
            time.sleep(10)

    for sql_data_k, sql_data_v in insert_data.items():
        sql_data = []
        sql_data.append(sql_data_k)
        for num in sql_data_v:
            sql_data.append(num)

        DBCli().targetdb_cli.insert(sql, sql_data)


def update_booke_ticket_event_hourly(days=0):
    """localytics预定机票各阶段事件(小时), ticket_book_event_hourly"""
    api_key = "dd633143c1a14867726b60a-812924b6-5b0b-11e6-71ff-002dea3c3994"
    api_secret = "f91925eb1865c8431589ff2-81292808-5b0b-11e6-71ff-002dea3c3994"
    # api_key = "0d2eb34de63f71462c15f0e-3f4088c2-5f00-11e6-7216-002dea3c3994"
    # api_secret = "2049d2d0815af8273eff9e4-3f408e30-5f00-11e6-7216-002dea3c3994"
    api_root = "https://api.localytics.com/v1/query"
    # app_id = "2c64c068203c5033ddb127f-c76c5cc2-582a-11e5-07bf-00deb82fd81f"
    app_id_android = "2c64c068203c5033ddb127f-c76c5cc2-582a-11e5-07bf-00deb82fd81f"
    app_id_ios = "c0b8588071fc960755ee311-9ac01816-582a-11e5-ba3c-0013a62af900"
    event_list = ["ticket.open", "ticket.query", "ticket.list.detail.click", "ticket.detail.order.online",
                  "ticket.order.pay.start", "ticket.order.pay.succ", "open"]
    hbdt_event = []
    pv_uv = ["sessions_per_event", "users"]

    # start_date = str(datetime.date(2016, 1, 1))
    # end_date = DateUtil.date2str(DateUtil.get_date_before_days(1), '%Y-%m-%d')
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')

    for event in event_list:
        hbdt_event.append("ios." + event)
        hbdt_event.append("android." + event)

    insert_data = defaultdict(list)

    sql = """
            insert into ticket_book_event_hourly (
            s_day,
            hour,
            ios_ticket_open_pv,
            ios_ticket_open_uv,
            android_ticket_open_pv,
            android_ticket_open_uv,
            ios_ticket_query_pv,
            ios_ticket_query_uv,
            android_ticket_query_pv,
            android_ticket_query_uv,
            ios_ticket_list_detail_click_pv,
            ios_ticket_list_detail_click_uv,
            android_ticket_list_detail_click_pv,
            android_ticket_list_detail_click_uv,
            ios_ticket_detail_order_online_pv,
            ios_ticket_detail_order_online_uv,
            android_ticket_detail_order_online_pv,
            android_ticket_detail_order_online_uv,
            ios_ticket_order_pay_start_pv,
            ios_ticket_order_pay_start_uv,
            android_ticket_order_pay_start_pv,
            android_ticket_order_pay_start_uv,
            ios_ticket_order_pay_succ_pv,
            ios_ticket_order_pay_succ_uv,
            android_ticket_order_pay_succ_pv,
            android_ticket_order_pay_succ_uv,
            ios_open_pv,
            ios_open_uv,
            android_open_pv,
            android_open_uv
            ) values (
            %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

    for event in hbdt_event:
        if event.startswith("ios"):
            app_id = app_id_ios
        elif event.startswith("android"):
            app_id = app_id_android

        for dim in pv_uv:

            data_params = {"app_id": app_id, "dimensions": "hour_of_day", "metrics": dim}

            conditions = {"event_name": event, "day": ["between", start_date, start_date]}

            data_params["conditions"] = json.dumps(conditions)
            try:
                r = requests.get(api_root, auth=(api_key, api_secret), params=data_params, timeout=240)
                result = r.json()
                if r.status_code == 429:
                    raise AssertionError
                data = result["results"]
            except Exception:
                raise AssertionError

            for d in data:
                insert_data[d["hour_of_day"]].append(d[dim])

    hour_data = []
    for sql_data_k, sql_data_v in sorted(insert_data.iteritems(), key=lambda a: a[0], reverse=False):
        new_tmp_data = list()
        new_tmp_data.append(start_date)
        new_tmp_data.append(int(sql_data_k.split(":")[0]))
        new_tmp_data.extend(sql_data_v)
        hour_data.append(new_tmp_data)

    DBCli().targetdb_cli.batch_insert(sql, hour_data)


def update_ios_android_newuser_daily(days=0):
    """ios android新用户, ticket_query_newuser_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    api_key = "dd633143c1a14867726b60a-812924b6-5b0b-11e6-71ff-002dea3c3994"
    api_secret = "f91925eb1865c8431589ff2-81292808-5b0b-11e6-71ff-002dea3c3994"

    # api_key = "0d2eb34de63f71462c15f0e-3f4088c2-5f00-11e6-7216-002dea3c3994"
    # api_secret = "2049d2d0815af8273eff9e4-3f408e30-5f00-11e6-7216-002dea3c3994"

    api_root = "https://api.localytics.com/v1/query"

    app_id_android = "2c64c068203c5033ddb127f-c76c5cc2-582a-11e5-07bf-00deb82fd81f"
    app_id_ios = "c0b8588071fc960755ee311-9ac01816-582a-11e5-ba3c-0013a62af900"
    insert_data = [start_date]
    hbdt_event = ["android.ticket.query", "ios.ticket.query"]

    insert_sql = """
        insert into ticket_query_newuser_daily (s_day, android_ticket_query_num, android_ticket_query_new_num,
        ios_ticket_query_num, ios_ticket_query_new_num, createtime, updatetime) values
        (%s, %s, %s, %s, %s , now(), now())
        on duplicate key update updatetime = now(),
        android_ticket_query_num = VALUES(android_ticket_query_num),
        android_ticket_query_new_num = VALUES(android_ticket_query_new_num),
        ios_ticket_query_num = VALUES(ios_ticket_query_num),
        ios_ticket_query_new_num = VALUES(ios_ticket_query_new_num)
    """

    for event in hbdt_event:
        if event.startswith("ios"):
            app_id = app_id_ios
        elif event.startswith("android"):
            app_id = app_id_android

        data_params = {"app_id": app_id, "dimensions": "day, new_device", "metrics": "users"}

        conditions = {"event_name": event, "day": ["between", start_date, start_date]}

        data_params["conditions"] = json.dumps(conditions)
        r = requests.get(api_root, auth=(api_key, api_secret), params=data_params, timeout=240)
        result_data = r.json()["results"]
        for result in result_data:
            insert_data.append(result["users"])

    DBCli().targetdb_cli.insert(insert_sql, insert_data)


if __name__ == "__main__":
    # update_ios_android_newuser_daily(1)
    # update_ios_android_newuser_daily(1)
    # update_booke_ticket_event_hourly(1)
    update_booke_ticket_event_hourly(1)
    # update_ios_android_newuser_daily(3)
    # update_booke_ticket_event_hourly(3)
    # i = 368
    # while 1 < 1500:
    #     update_ios_android_newuser_daily(i)
    #     i += 1
