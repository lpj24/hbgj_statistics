# -*- coding: utf-8 -*-
import requests
import json
from collections import defaultdict
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import datetime
import time


def hbdt_event(days=0):
    """更新localytics航班动态事件, hbdt_event"""
    api_key = "dd633143c1a14867726b60a-812924b6-5b0b-11e6-71ff-002dea3c3994"
    api_secret = "f91925eb1865c8431589ff2-81292808-5b0b-11e6-71ff-002dea3c3994"

    api_root = "https://api.localytics.com/v1/query"
    # app_id = "2c64c068203c5033ddb127f-c76c5cc2-582a-11e5-07bf-00deb82fd81f"
    app_id_android = "2c64c068203c5033ddb127f-c76c5cc2-582a-11e5-07bf-00deb82fd81f"
    app_id_ios = "c0b8588071fc960755ee311-9ac01816-582a-11e5-ba3c-0013a62af900"
    event_list = ["status.query.open", "status.flyno.query", "status.list.attention.click"]
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
        insert into hbdt_event (
            s_day,
            ios_status_query_open_pv,
            ios_status_query_open_uv,
            android_status_query_open_pv,
            android_status_query_open_uv,
            ios_status_flyno_query_pv,
            ios_status_flyno_query_uv,
            android_status_flyno_query_pv,
            android_status_flyno_query_uv,
            ios_status_list_attention_click_pv,
            ios_status_list_attention_click_uv,
            android_status_list_attention_click_pv,
            android_status_list_attention_click_uv) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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

            for d in data:
                insert_data[d["day"]].append(d[dim])

    for sql_data_k, sql_data_v in insert_data.items():
        sql_data = list()
        sql_data.append(sql_data_k)
        for num in sql_data_v:
            sql_data.append(num)

        DBCli().targetdb_cli.insert(sql, sql_data)

if __name__ == "__main__":
    # import time
    # time.sleep(1 * 60 * 60)
    # hbdt_event(34)

    # i = 419
    # while i < 600:
    #     print i
    #     try:
    #         hbdt_event(i)
    #     except AssertionError:
    #         time.sleep(1 * 60 * 60)
    #         continue
    #     i += 1

    hbdt_event(3)


