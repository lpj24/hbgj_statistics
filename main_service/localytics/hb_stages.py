# -*- coding: utf-8 -*-
import requests
import json
from dbClient.dateutil import DateUtil
from dbClient.db_client import DBCli


def update_hbgj_stages_daily(days=0):
    """更新分期付款localytics, weex_installment_pay_daily"""
    query_date = DateUtil.date2str(DateUtil.get_date_before_days(int(days) * 1), '%Y-%m-%d')
    api_key = "dd633143c1a14867726b60a-812924b6-5b0b-11e6-71ff-002dea3c3994"
    api_secret = "f91925eb1865c8431589ff2-81292808-5b0b-11e6-71ff-002dea3c3994"
    api_root = "https://api.localytics.com/v1/query"
    app_id_android = "2c64c068203c5033ddb127f-c76c5cc2-582a-11e5-07bf-00deb82fd81f"
    app_id_ios = "c0b8588071fc960755ee311-9ac01816-582a-11e5-ba3c-0013a62af900"
    event_list = ['weex.installment.pay.start', 'weex.installment.activated', 'weex.installment.pay.success']
    insert_result = [query_date, ]
    metrics_pv_uv = ['sessions_per_event', 'users']

    insert_sql = """
        insert into weex_installment_pay_daily (
            s_day,
            ios_weex_installment_pay_start_pv,
            ios_weex_installment_pay_start_uv,
            ios_weex_installment_pay_activated_pv,
            ios_weex_installment_pay_activated_uv,
            ios_weex_installment_pay_success_pv,
            ios_weex_installment_pay_success_uv,
            createtime,
            updatetime
        ) values (
            %s, %s, %s, %s, %s, %s, %s, now(), now()
        )
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        ios_weex_installment_pay_start_pv = values(ios_weex_installment_pay_start_pv),
        ios_weex_installment_pay_start_uv = values(ios_weex_installment_pay_start_uv),
        ios_weex_installment_pay_activated_pv = values(ios_weex_installment_pay_activated_pv),
        ios_weex_installment_pay_activated_uv = values(ios_weex_installment_pay_activated_uv),
        ios_weex_installment_pay_success_pv = values(ios_weex_installment_pay_success_pv),
        ios_weex_installment_pay_success_uv = values(ios_weex_installment_pay_success_uv)
    """

    for p in ['ios.', ]:
        event_list = [p + e for e in event_list]
        a_id = app_id_ios if p.count('ios') else app_id_android
        for e in event_list:
            phone_event = e
            for m in metrics_pv_uv:

                data_params = {"app_id": a_id, "dimensions": "day", "metrics": m}

                conditions = {"event_name": phone_event, "day": ["between", query_date, query_date]}

                data_params["conditions"] = json.dumps(conditions)

                r = requests.get(api_root, auth=(api_key, api_secret), params=data_params, timeout=240)
                result = r.json()
                print result
                if r.status_code == 429:
                    raise AssertionError
                insert_result.append((result["results"][0])[m])
    DBCli().targetdb_cli.insert(insert_sql, insert_result)
    return __file__

if __name__ == "__main__":
    i = 8
    while 1:
        update_hbgj_stages_daily(i)
        i += 1