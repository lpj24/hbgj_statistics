# -*- coding: utf-8 -*-
from dbClient.dateutil import DateUtil
from dbClient.db_client import DBCli
from core import request_uv


def update_weex_phoneverify(days=1):
    """更新实名认证流程, weex_installment_nameauth_daily"""
    query_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 1), '%Y-%m-%d')
    event_list = ['weex.installment.nameauth.bankcardnumber.open', 'weex.installment.nameauth.bankcardinfo.open',
                  'weex.installment.nameauth.sms.open', 'weex.installment.nameauth.sms.success']

    insert_sql = """
        insert into weex_installment_nameauth_daily (
            s_day,
            ios_weex_installment_nameauth_bankcardnumber_open_uv,
            ios_weex_installment_nameauth_bankcardinfo_open_uv,
            ios_weex_installment_nameauth_sms_open_uv,
            ios_weex_installment_nameauth_sms_success_uv,
            
            android_weex_installment_nameauth_bankcardnumber_open_uv,
            android_weex_installment_nameauth_bankcardinfo_open_uv,
            android_weex_installment_nameauth_sms_open_uv,
            android_weex_installment_nameauth_sms_success_uv,
            createtime,
            updatetime
        ) values (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()
        )
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        ios_weex_installment_nameauth_bankcardnumber_open_uv = values(ios_weex_installment_nameauth_bankcardnumber_open_uv),
        ios_weex_installment_nameauth_bankcardinfo_open_uv = values(ios_weex_installment_nameauth_bankcardinfo_open_uv),
        ios_weex_installment_nameauth_sms_open_uv = values(ios_weex_installment_nameauth_sms_open_uv),
        ios_weex_installment_nameauth_sms_success_uv = values(ios_weex_installment_nameauth_sms_success_uv), 
        
        android_weex_installment_nameauth_bankcardnumber_open_uv = values(android_weex_installment_nameauth_bankcardnumber_open_uv),
        android_weex_installment_nameauth_bankcardinfo_open_uv = values(android_weex_installment_nameauth_bankcardinfo_open_uv),
        android_weex_installment_nameauth_sms_open_uv = values(android_weex_installment_nameauth_sms_open_uv),
        android_weex_installment_nameauth_sms_success_uv = values(android_weex_installment_nameauth_sms_success_uv)

    """
    insert_data = [query_date]

    for p in ['ios.', 'android.']:
        for e in event_list:
            # localytics客户端有问题, android需要特殊处理
            if p.count('ios'):
                e = p + e
            uv_data = request_uv(query_date, query_date, e, 'day')
            if len(uv_data) > 0:
                insert_data.append(uv_data[0]['users'])
            else:
                insert_data.append(0)

    DBCli().targetdb_cli.insert(insert_sql, insert_data)


if __name__ == '__main__':
    for i in xrange(1, 9):
        update_weex_phoneverify(i)
    # update_weex_phoneverify(15)