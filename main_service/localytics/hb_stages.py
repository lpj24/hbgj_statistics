# -*- coding: utf-8 -*-
from dbClient.dateutil import DateUtil
from dbClient.db_client import DBCli
from collections import defaultdict
from core import *


def update_hbgj_stages_daily(days=0):
    """更新分期付款localytics, weex_installment_pay_daily"""

    query_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 3), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_before_days(int(days) * 1), '%Y-%m-%d')
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
    insert_result = defaultdict(list)
    for p in ['ios.', ]:
        event_list = [p + e for e in event_list]
        for e in event_list:
            pv_data = request_pv(query_date, end_date, e, 'day')
            uv_data = request_uv(query_date, end_date, e, 'day')
            for pv_detail, uv_detail in zip(pv_data, uv_data):
                insert_result[pv_detail['day']].append(pv_detail['sessions_per_event'])
                insert_result[pv_detail['day']].append(uv_detail['users'])

    for k, v in insert_result.items():
        v.insert(0, k)
        insert_data.append(v)
    DBCli().targetdb_cli.batchInsert(insert_sql, insert_data)
    return __file__


def update_weex_activated_type_daily(days=0):
    """更新分期付款localytics, weex_installment_activated_type_daily"""
    event_list = ['ios.weex.installment.activated']
    query_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 3), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_before_days(int(days) * 1), '%Y-%m-%d')
    insert_sql = """
        insert into weex_installment_activated_type_daily (
            s_day,
            ios_weex_installment_activated_installment_pv,
            ios_weex_installment_activated_installment_uv,
            ios_weex_installment_activated_mylimit_pv,
            ios_weex_installment_activated_mylimit_uv,
            createtime,
            updatetime
        ) values (
            %s, %s, %s, %s, %s, now(), now()
        )
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        ios_weex_installment_activated_installment_pv = values(ios_weex_installment_activated_installment_pv),
        ios_weex_installment_activated_installment_uv = values(ios_weex_installment_activated_installment_uv),
        ios_weex_installment_activated_mylimit_pv = values(ios_weex_installment_activated_mylimit_pv),
        ios_weex_installment_activated_mylimit_uv = values(ios_weex_installment_activated_mylimit_uv)
    """
    insert_result = defaultdict(list)
    insert_data = []
    for e in event_list:
        pv_data = request_pv(query_date, end_date, e, 'day, a:type')
        uv_data = request_uv(query_date, end_date, e, 'day, a:type')
        for pv_detail, uv_detail in zip(pv_data, uv_data):
            insert_result[pv_detail['day']].append(pv_detail['sessions_per_event'])
            insert_result[pv_detail['day']].append(uv_detail['users'])

    for k, v in insert_result.items():
        v.insert(0, k)
        insert_data.append(v)
    DBCli().targetdb_cli.batchInsert(insert_sql, insert_data)
    return __file__

if __name__ == "__main__":
    # update_hbgj_stages_daily(1)
    update_weex_activated_type_daily(1)