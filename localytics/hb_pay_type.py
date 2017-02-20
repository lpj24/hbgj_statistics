# -*- coding: utf-8 -*-
import requests
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import json
import time
import datetime


def hb_pay_type(days=0):
    api_key = "dd633143c1a14867726b60a-812924b6-5b0b-11e6-71ff-002dea3c3994"
    api_secret = "f91925eb1865c8431589ff2-81292808-5b0b-11e6-71ff-002dea3c3994"
    api_root = "https://api.localytics.com/v1/query"
    # app_id = "2c64c068203c5033ddb127f-c76c5cc2-582a-11e5-07bf-00deb82fd81f"
    app_id_android = "2c64c068203c5033ddb127f-c76c5cc2-582a-11e5-07bf-00deb82fd81f"
    app_id_ios = "c0b8588071fc960755ee311-9ac01816-582a-11e5-ba3c-0013a62af900"

    android_ticket_order_pay_start = "android.ticket.order.pay.start"       #支付开始
    android_ticket_order_pay_succ = "android.ticket.order.pay.succ"         #支付成功
    android_ticket_order_pay_fail = "android.ticket.order.pay.fail"         #支付失败

    ios_ticket_order_pay_start = "ios.ticket.order.pay.start"       #支付开始
    ios_ticket_order_pay_succ = "ios.ticket.order.pay.succ"         #支付成功
    ios_ticket_order_pay_fail = "ios.ticket.order.pay.fail"         #支付失败

    event_list = [android_ticket_order_pay_start, android_ticket_order_pay_succ, android_ticket_order_pay_fail, ios_ticket_order_pay_start,
                  ios_ticket_order_pay_succ, ios_ticket_order_pay_fail]
    dimensions = ["occurrences", "sessions_per_event", "users"]

    # pay_type_list = {
    #     "flypay.creditcard": {"paystart_o": 0, "paystart_s": 0, "paystart_u":0, "paysucc_o": 0, "paysucc_s": 0, "paysucc_u":0, "payfail_o": 0, "payfail_s": 0, "payfail_u":0},
    #     "alipay": {"paystart_o": 0, "paystart_s": 0, "paystart_u":0, "paysucc_o": 0, "paysucc_s": 0, "paysucc_u":0, "payfail_o": 0, "payfail_s": 0, "payfail_u":0},
    #     "weixinpay": {"paystart_o": 0, "paystart_s": 0, "paystart_u":0, "paysucc_o": 0, "paysucc_s": 0, "paysucc_u":0, "payfail_o": 0, "payfail_s": 0, "payfail_u":0},
    #     "flypay.depositcard": {"paystart_o": 0, "paystart_s": 0, "paystart_u":0, "paysucc_o": 0, "paysucc_s": 0, "paysucc_u":0, "payfail_o": 0, "payfail_s": 0, "payfail_u":0},
    #     "flypay.creditcard+balance": {"paystart_o": 0, "paystart_s": 0, "paystart_u":0, "paysucc_o": 0, "paysucc_s": 0, "paysucc_u":0, "payfail_o": 0, "payfail_s": 0, "payfail_u":0},
    #     "alipay+balance": {"paystart_o": 0, "paystart_s": 0, "paystart_u":0, "paysucc_o": 0, "paysucc_s": 0, "paysucc_u":0, "payfail_o": 0, "payfail_s": 0, "payfail_u":0},
    #     "weixinpay+balance": {"paystart_o": 0, "paystart_s": 0, "paystart_u":0, "paysucc_o": 0, "paysucc_s": 0, "paysucc_u":0, "payfail_o": 0, "payfail_s": 0, "payfail_u":0},
    #     "flypay.depositcard+balance": {"paystart_o": 0, "paystart_s": 0, "paystart_u":0, "paysucc_o": 0, "paysucc_s": 0, "paysucc_u":0, "payfail_o": 0, "payfail_s": 0, "payfail_u":0},
    #     "flypay.creditcard+coupons": {"paystart_o": 0, "paystart_s": 0, "paystart_u":0, "paysucc_o": 0, "paysucc_s": 0, "paysucc_u":0, "payfail_o": 0, "payfail_s": 0, "payfail_u":0},
    #     "flypay.depositcard+coupons": {"paystart_o": 0, "paystart_s": 0, "paystart_u":0, "paysucc_o": 0, "paysucc_s": 0, "paysucc_u":0, "payfail_o": 0, "payfail_s": 0, "payfail_u":0},
    #     "alipay+coupons": {"paystart_o": 0, "paystart_s": 0, "paystart_u":0, "paysucc_o": 0, "paysucc_s": 0, "paysucc_u":0, "payfail_o": 0, "payfail_s": 0, "payfail_u":0},
    #     "flypay.creditcard+balance+coupons": {"paystart_o": 0, "paystart_s": 0, "paystart_u":0, "paysucc_o": 0, "paysucc_s": 0, "paysucc_u":0, "payfail_o": 0, "payfail_s": 0, "payfail_u":0},
    #     "alipay+balance+coupons": {"paystart_o": 0, "paystart_s": 0, "paystart_u":0, "paysucc_o": 0, "paysucc_s": 0, "paysucc_u":0, "payfail_o": 0, "payfail_s": 0, "payfail_u":0},
    #     "corppay": {"paystart_o": 0, "paystart_s": 0, "paystart_u":0, "paysucc_o": 0, "paysucc_s": 0, "paysucc_u":0, "payfail_o": 0, "payfail_s": 0, "payfail_u":0},
    #     "weixinpay+coupons": {"paystart_o": 0, "paystart_s": 0, "paystart_u":0, "paysucc_o": 0, "paysucc_s": 0, "paysucc_u":0, "payfail_o": 0, "payfail_s": 0, "payfail_u":0},
    # }

    data_params = {}
    # start_date = datetime.date(2016, 8, 24)
    # end_date = datetime.date(2016, 9, 1)

    start_date = DateUtil.get_date_before_days(days)
    end_date = start_date
    sql = """
        insert into hbgj_event_orderpay_paytype_ios_android_daily (s_day, pay_type, android_paystart_o, android_paystart_s,
        android_paystart_u, android_paysucc_o, android_paysucc_s, android_paysucc_u, android_payfail_o ,android_payfail_s, android_payfail_u,
        ios_paystart_o, ios_paystart_s,
        ios_paystart_u, ios_paysucc_o, ios_paysucc_s, ios_paysucc_u, ios_payfail_o ,ios_payfail_s, ios_payfail_u,
        createtime, updatetime) values
        (%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,now(),now())
    """

    while start_date <= end_date:
        pay_type_list = {
            "flypay.creditcard": {"android_paystart_o": 0, "android_paystart_s": 0, "android_paystart_u": 0, "android_paysucc_o": 0, "android_paysucc_s": 0,
                                  "android_paysucc_u": 0, "android_payfail_o": 0, "android_payfail_s": 0, "android_payfail_u": 0, "ios_paystart_o": 0, "ios_paystart_s": 0,
                                  "ios_paystart_u": 0, "ios_paysucc_o": 0, "ios_paysucc_s": 0, "ios_paysucc_u": 0, "ios_payfail_o": 0, "ios_payfail_s": 0, "ios_payfail_u": 0},
            "alipay": {"android_paystart_o": 0, "android_paystart_s": 0, "android_paystart_u": 0, "android_paysucc_o": 0, "android_paysucc_s": 0,
                      "android_paysucc_u": 0, "android_payfail_o": 0, "android_payfail_s": 0, "android_payfail_u": 0, "ios_paystart_o": 0, "ios_paystart_s": 0,
                      "ios_paystart_u": 0, "ios_paysucc_o": 0, "ios_paysucc_s": 0, "ios_paysucc_u": 0, "ios_payfail_o": 0, "ios_payfail_s": 0, "ios_payfail_u": 0},
            "weixinpay": {"android_paystart_o": 0, "android_paystart_s": 0, "android_paystart_u": 0, "android_paysucc_o": 0, "android_paysucc_s": 0,
                          "android_paysucc_u": 0, "android_payfail_o": 0, "android_payfail_s": 0, "android_payfail_u": 0, "ios_paystart_o": 0, "ios_paystart_s": 0,
                          "ios_paystart_u": 0, "ios_paysucc_o": 0, "ios_paysucc_s": 0, "ios_paysucc_u": 0, "ios_payfail_o": 0, "ios_payfail_s": 0, "ios_payfail_u": 0},
            "flypay.depositcard": {"android_paystart_o": 0, "android_paystart_s": 0, "android_paystart_u": 0, "android_paysucc_o": 0, "android_paysucc_s": 0,
                                  "android_paysucc_u": 0, "android_payfail_o": 0, "android_payfail_s": 0, "android_payfail_u": 0, "ios_paystart_o": 0, "ios_paystart_s": 0,
                                  "ios_paystart_u": 0, "ios_paysucc_o": 0, "ios_paysucc_s": 0, "ios_paysucc_u": 0, "ios_payfail_o": 0, "ios_payfail_s": 0, "ios_payfail_u": 0},
            "flypay.creditcard+balance": {"android_paystart_o": 0, "android_paystart_s": 0, "android_paystart_u": 0, "android_paysucc_o": 0, "android_paysucc_s": 0,
                                  "android_paysucc_u": 0, "android_payfail_o": 0, "android_payfail_s": 0, "android_payfail_u": 0, "ios_paystart_o": 0, "ios_paystart_s": 0,
                                  "ios_paystart_u": 0, "ios_paysucc_o": 0, "ios_paysucc_s": 0, "ios_paysucc_u": 0, "ios_payfail_o": 0, "ios_payfail_s": 0, "ios_payfail_u": 0},
            "alipay+balance": {"android_paystart_o": 0, "android_paystart_s": 0, "android_paystart_u": 0, "android_paysucc_o": 0, "android_paysucc_s": 0,
                                  "android_paysucc_u": 0, "android_payfail_o": 0, "android_payfail_s": 0, "android_payfail_u": 0, "ios_paystart_o": 0, "ios_paystart_s": 0,
                                  "ios_paystart_u": 0, "ios_paysucc_o": 0, "ios_paysucc_s": 0, "ios_paysucc_u": 0, "ios_payfail_o": 0, "ios_payfail_s": 0, "ios_payfail_u": 0},
            "weixinpay+balance": {"android_paystart_o": 0, "android_paystart_s": 0, "android_paystart_u": 0, "android_paysucc_o": 0, "android_paysucc_s": 0,
                                  "android_paysucc_u": 0, "android_payfail_o": 0, "android_payfail_s": 0, "android_payfail_u": 0, "ios_paystart_o": 0, "ios_paystart_s": 0,
                                  "ios_paystart_u": 0, "ios_paysucc_o": 0, "ios_paysucc_s": 0, "ios_paysucc_u": 0, "ios_payfail_o": 0, "ios_payfail_s": 0, "ios_payfail_u": 0},
            "flypay.depositcard+balance": {"android_paystart_o": 0, "android_paystart_s": 0, "android_paystart_u": 0, "android_paysucc_o": 0, "android_paysucc_s": 0,
                                  "android_paysucc_u": 0, "android_payfail_o": 0, "android_payfail_s": 0, "android_payfail_u": 0, "ios_paystart_o": 0, "ios_paystart_s": 0,
                                  "ios_paystart_u": 0, "ios_paysucc_o": 0, "ios_paysucc_s": 0, "ios_paysucc_u": 0, "ios_payfail_o": 0, "ios_payfail_s": 0, "ios_payfail_u": 0},
            "flypay.creditcard+coupons": {"android_paystart_o": 0, "android_paystart_s": 0, "android_paystart_u": 0, "android_paysucc_o": 0, "android_paysucc_s": 0,
                                  "android_paysucc_u": 0, "android_payfail_o": 0, "android_payfail_s": 0, "android_payfail_u": 0, "ios_paystart_o": 0, "ios_paystart_s": 0,
                                  "ios_paystart_u": 0, "ios_paysucc_o": 0, "ios_paysucc_s": 0, "ios_paysucc_u": 0, "ios_payfail_o": 0, "ios_payfail_s": 0, "ios_payfail_u": 0},
            "flypay.depositcard+coupons": {"android_paystart_o": 0, "android_paystart_s": 0, "android_paystart_u": 0, "android_paysucc_o": 0, "android_paysucc_s": 0,
                                  "android_paysucc_u": 0, "android_payfail_o": 0, "android_payfail_s": 0, "android_payfail_u": 0, "ios_paystart_o": 0, "ios_paystart_s": 0,
                                  "ios_paystart_u": 0, "ios_paysucc_o": 0, "ios_paysucc_s": 0, "ios_paysucc_u": 0, "ios_payfail_o": 0, "ios_payfail_s": 0, "ios_payfail_u": 0},
            "alipay+coupons": {"android_paystart_o": 0, "android_paystart_s": 0, "android_paystart_u": 0, "android_paysucc_o": 0, "android_paysucc_s": 0,
                                  "android_paysucc_u": 0, "android_payfail_o": 0, "android_payfail_s": 0, "android_payfail_u": 0, "ios_paystart_o": 0, "ios_paystart_s": 0,
                                  "ios_paystart_u": 0, "ios_paysucc_o": 0, "ios_paysucc_s": 0, "ios_paysucc_u": 0, "ios_payfail_o": 0, "ios_payfail_s": 0, "ios_payfail_u": 0},
            "flypay.creditcard+balance+coupons": {"android_paystart_o": 0, "android_paystart_s": 0, "android_paystart_u": 0, "android_paysucc_o": 0, "android_paysucc_s": 0,
                                  "android_paysucc_u": 0, "android_payfail_o": 0, "android_payfail_s": 0, "android_payfail_u": 0, "ios_paystart_o": 0, "ios_paystart_s": 0,
                                  "ios_paystart_u": 0, "ios_paysucc_o": 0, "ios_paysucc_s": 0, "ios_paysucc_u": 0, "ios_payfail_o": 0, "ios_payfail_s": 0, "ios_payfail_u": 0},
            "alipay+balance+coupons": {"android_paystart_o": 0, "android_paystart_s": 0, "android_paystart_u": 0, "android_paysucc_o": 0, "android_paysucc_s": 0,
                                  "android_paysucc_u": 0, "android_payfail_o": 0, "android_payfail_s": 0, "android_payfail_u": 0, "ios_paystart_o": 0, "ios_paystart_s": 0,
                                  "ios_paystart_u": 0, "ios_paysucc_o": 0, "ios_paysucc_s": 0, "ios_paysucc_u": 0, "ios_payfail_o": 0, "ios_payfail_s": 0, "ios_payfail_u": 0},
            "corppay": {"android_paystart_o": 0, "android_paystart_s": 0, "android_paystart_u": 0, "android_paysucc_o": 0, "android_paysucc_s": 0,
                                  "android_paysucc_u": 0, "android_payfail_o": 0, "android_payfail_s": 0, "android_payfail_u": 0, "ios_paystart_o": 0, "ios_paystart_s": 0,
                                  "ios_paystart_u": 0, "ios_paysucc_o": 0, "ios_paysucc_s": 0, "ios_paysucc_u": 0, "ios_payfail_o": 0, "ios_payfail_s": 0, "ios_payfail_u": 0},
            "weixinpay+coupons": {"android_paystart_o": 0, "android_paystart_s": 0, "android_paystart_u": 0, "android_paysucc_o": 0, "android_paysucc_s": 0,
                                  "android_paysucc_u": 0, "android_payfail_o": 0, "android_payfail_s": 0, "android_payfail_u": 0, "ios_paystart_o": 0, "ios_paystart_s": 0,
                                  "ios_paystart_u": 0, "ios_paysucc_o": 0, "ios_paysucc_s": 0, "ios_paysucc_u": 0, "ios_payfail_o": 0, "ios_payfail_s": 0, "ios_payfail_u": 0},
        }
        s_day = DateUtil.date2str(start_date, '%Y-%m-%d')
        for event in event_list:
            if event.startswith("android"):
                app_id = app_id_android
                field = "android" + "_" + event.split(".")[-2] + event.split(".")[-1]
            else:
                app_id = app_id_ios
                field = "ios" + "_" + event.split(".")[-2] + event.split(".")[-1]
            data_params = {"app_id": app_id, "dimensions": "a:paytype", "limit": 15}

            for dim in dimensions:
                query_field = field + "_" + dim[0]
                conditions = {"event_name": event, "day": ["between", s_day, s_day]}
                data_params["conditions"] = json.dumps(conditions)

                data_params["metrics"] = dim
                data_params["order"] = dim
                try:
                    r = requests.get(api_root, auth=(api_key, api_secret), params=data_params, timeout=60)
                    result = r.json()
                    data = result["results"]
                except Exception:
                    return
                    # time.sleep(60*30)
                    # hb_pay_type(1)
                for d in data:
                    if pay_type_list.get(d["a:paytype"]):
                        pay_type_list.get(d["a:paytype"])[query_field] = d[dim]
                time.sleep(10)

        for k, v in pay_type_list.items():
            insert_data = [s_day, k, v["android_paystart_o"], v["android_paystart_s"], v["android_paystart_u"], v["android_paysucc_o"], v["android_paysucc_s"], v["android_paysucc_u"],
                           v["android_payfail_o"], v["android_payfail_s"], v["android_payfail_u"], v["ios_paystart_o"],
                           v["ios_paystart_s"], v["ios_paystart_u"], v["ios_paysucc_o"], v["ios_paysucc_s"], v["ios_paysucc_u"],
                           v["ios_payfail_o"
                             ""], v["ios_payfail_s"], v["ios_payfail_u"]]
            DBCli().targetdb_cli.insert(sql, insert_data)
        time.sleep(100*2)
        start_date = DateUtil.add_days(start_date, 1)

if __name__ == "__main__":
    try:
        hb_pay_type(1)
    except Exception as e:
        print e.message