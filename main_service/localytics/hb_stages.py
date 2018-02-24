# -*- coding: utf-8 -*-
from dbClient.dateutil import DateUtil
from dbClient.db_client import DBCli
from collections import defaultdict
from core import *


def update_hbgj_stages_daily(days=0):
    """分期付款localytics, weex_installment_pay_daily"""

    query_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 1), '%Y-%m-%d')
    event_list = ['weex.installment.pay.start', 'weex.installment.activated', 'weex.installment.pay.success']
    insert_data = []

    insert_sql = """
        insert into weex_installment_pay_daily (
            s_day,
            ios_weex_installment_pay_start_pv,
            ios_weex_installment_pay_start_uv,
            ios_weex_installment_pay_activated_pv,
            ios_weex_installment_pay_activated_uv,
            ios_weex_installment_pay_success_pv,
            ios_weex_installment_pay_success_uv,

            android_weex_installment_pay_start_pv,
            android_weex_installment_pay_start_uv,
            android_weex_installment_pay_activated_pv,
            android_weex_installment_pay_activated_uv,
            android_weex_installment_pay_success_pv,
            android_weex_installment_pay_success_uv,
            createtime,
            updatetime
        ) values (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()
        )
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        ios_weex_installment_pay_start_pv = values(ios_weex_installment_pay_start_pv),
        ios_weex_installment_pay_start_uv = values(ios_weex_installment_pay_start_uv),
        ios_weex_installment_pay_activated_pv = values(ios_weex_installment_pay_activated_pv),
        ios_weex_installment_pay_activated_uv = values(ios_weex_installment_pay_activated_uv),
        ios_weex_installment_pay_success_pv = values(ios_weex_installment_pay_success_pv),
        ios_weex_installment_pay_success_uv = values(ios_weex_installment_pay_success_uv),
        android_weex_installment_pay_start_pv = values(android_weex_installment_pay_start_pv),
        android_weex_installment_pay_start_uv = values(android_weex_installment_pay_start_uv),
        android_weex_installment_pay_activated_pv = values(android_weex_installment_pay_activated_pv),
        android_weex_installment_pay_activated_uv = values(android_weex_installment_pay_activated_uv),
        android_weex_installment_pay_success_pv = values(android_weex_installment_pay_success_pv),
        android_weex_installment_pay_success_uv = values(android_weex_installment_pay_success_uv)
    """
    insert_result = defaultdict(list)
    for p in ['ios.', '']:
        for e in event_list:
            pv_data = request_pv(query_date, query_date, p + e, 'day')
            uv_data = request_uv(query_date, query_date, p + e, 'day')
            if len(pv_data) >= 1:
                insert_result[query_date].append(pv_data[0]['sessions_per_event'])
            else:
                insert_result[query_date].append(0)

            if len(uv_data) >= 1:
                insert_result[query_date].append(uv_data[0]['users'])
            else:
                insert_result[query_date].append(0)
    for k, v in insert_result.items():
        v.insert(0, k)
        insert_data.append(v)

    DBCli().targetdb_cli.batch_insert(insert_sql, insert_data)


def update_weex_activated_type_daily(days=0):
    """分期付款localytics, weex_installment_activated_type_daily"""
    event_list = ['weex.installment.activated']
    query_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 1), '%Y-%m-%d')
    insert_sql = """
        insert into weex_installment_activated_type_daily (
            s_day,
            ios_weex_installment_activated_installment_pv,
            ios_weex_installment_activated_mylimit_pv,
            ios_weex_installment_activated_installment_uv,
            ios_weex_installment_activated_mylimit_uv,

            android_weex_installment_activated_installment_pv,
            android_weex_installment_activated_mylimit_pv,
            android_weex_installment_activated_installment_uv,
            android_weex_installment_activated_mylimit_uv,
            createtime,
            updatetime
        ) values (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()
        )
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        ios_weex_installment_activated_installment_pv = values(ios_weex_installment_activated_installment_pv),
        ios_weex_installment_activated_mylimit_pv = values(ios_weex_installment_activated_mylimit_pv),
        ios_weex_installment_activated_installment_uv = values(ios_weex_installment_activated_installment_uv),
        ios_weex_installment_activated_mylimit_uv = values(ios_weex_installment_activated_mylimit_uv),

        android_weex_installment_activated_installment_pv = values(android_weex_installment_activated_installment_pv),
        android_weex_installment_activated_mylimit_pv = values(android_weex_installment_activated_mylimit_pv),
        android_weex_installment_activated_installment_uv = values(android_weex_installment_activated_installment_uv),
        android_weex_installment_activated_mylimit_uv = values(android_weex_installment_activated_mylimit_uv)
    """
    insert_result = defaultdict(list)
    insert_data = []
    for p in ['ios.', '']:
        for e in event_list:
            pv_data = request_pv(query_date, query_date, p + e, 'day, a:type')
            uv_data = request_uv(query_date, query_date, p + e, 'day, a:type')

            if len(pv_data) >= 1:
                insert_result[query_date].append(pv_data[0]['sessions_per_event'])
                if len(pv_data) < 2:
                    insert_result[query_date].append(0)
                else:
                    insert_result[query_date].append(pv_data[1]['sessions_per_event'])
            else:
                insert_result[query_date].append(0)
                insert_result[query_date].append(0)

            if len(uv_data) >= 1:
                insert_result[query_date].append(uv_data[0]['users'])
                if len(uv_data) < 2:
                    insert_result[query_date].append(0)
                else:
                    insert_result[query_date].append(uv_data[1]['users'])
            else:
                insert_result[query_date].append(0)
                insert_result[query_date].append(0)

    for k, v in insert_result.items():
        v.insert(0, k)
        insert_data.append(v)
    DBCli().targetdb_cli.batch_insert(insert_sql, insert_data)


if __name__ == "__main__":
    # update_hbgj_stages_daily(1)
    # update_hbgj_stages_daily(1)
    update_weex_activated_type_daily(6)