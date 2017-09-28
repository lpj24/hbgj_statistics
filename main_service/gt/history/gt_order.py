# -*- coding: utf-8 -*-
from pymongo import Connection
from dbClient.dateutil import DateUtil
import datetime
from dbClient.db_client import DBCli
from sql.gt_sqlHandlers import gt_new_order_sql
import time
import requests


def mongo_his():
    conn = Connection('221.235.53.169', 27017)
    db = conn["gtgj"]
    coll = db["sub_order"]
    coll_order = db["order"]
    # coll_order_his = db["order_history"]
    # start_date = DateUtil.date2str(DateUtil.get_date_before_days(1), '%Y%m%d')
    # end_date = DateUtil.date2str(DateUtil.get_date_before_days(0), '%Y%m%d')
    start_date = datetime.date(2016, 7, 11)

    last_date = datetime.date(2013, 6, 6)

    while start_date >= last_date:
        end_date = DateUtil.add_days(start_date, 1)
        tmp_start_date = int(DateUtil.date2str(start_date, "%Y%m%d"))
        tmp_end_date = int(DateUtil.date2str(end_date, "%Y%m%d"))

        query_data_total = coll.aggregate([
            {"$match": {"order_date": {"$gte": tmp_start_date, "$lt": tmp_end_date},
                        "status": {"$nin": [u'取消订单', u'取消改签']}
                        }},
            {"$group": {"_id": "$order_date",
                        "gmv": {"$sum": "$price"}, "ticket_num": {"$sum": 1}}
             }
        ])
        query_data_total_order = coll_order.find({"i_status": {"$nin": ["2"]}, "order_date": tmp_start_date}).count()

        query_data_ios = coll.aggregate([
            {"$match": {"order_date": {"$gte": tmp_start_date, "$lt": tmp_end_date}, "platform": "ios",
                        "status": {"$nin": [u'取消订单', u'取消改签']}}},
            {"$group": {"_id": "$order_date",
                        "gmv": {"$sum": "$price"}, "ticket_num": {"$sum": 1}}
             }
        ])

        query_data_ios_order = coll_order.find({"i_status": {"$nin": ["2"]}, "order_date": tmp_start_date, "platform": "ios"}).count()

        query_data_android = coll.aggregate([
            {"$match": {"order_date": {"$gte": tmp_start_date, "$lt": tmp_end_date}, "platform": "android",
                        "status": {"$nin": [u'取消订单', u'取消改签']}}},
            {"$group": {"_id": "$order_date",
                        "gmv": {"$sum": "$price"}, "ticket_num": {"$sum": 1}}
             }
        ])
        query_data_android_order = coll_order.find({"i_status": {"$nin": ["2"]}, "order_date": tmp_start_date,
                                                     "platform": "android"}).count()

        result = (query_data_total["result"])
        result_ios = (query_data_ios["result"])
        result_android = (query_data_android["result"])

        if len(result) <= 0: result = [{"order_num": [], "_id": [], "gmv": 0, "ticket_num": 0}]
        if len(result_ios) <= 0: result_ios = [{"order_num": [], "_id": [], "gmv": 0, "ticket_num": 0}]
        if len(result_android) <= 0: result_android = [{"order_num": [], "_id": [], "gmv": 0, "ticket_num": 0}]

        for i in xrange(len(result)):
            tmp_result = result[i]
            tmp_result_ios = result_ios[i]
            tmp_result_android = result_android[i]

            s_day = str(tmp_result["_id"])
            tmp_tuple = (s_day[0:4] + "-" + s_day[4:6] + "-" + s_day[6:],
                         tmp_result["ticket_num"], query_data_total_order, tmp_result["gmv"],
                         tmp_result_ios["ticket_num"], query_data_ios_order, tmp_result_ios["gmv"],
                         tmp_result_android["ticket_num"], query_data_android_order,
                         tmp_result_android["gmv"]
                         )

            DBCli().targetdb_cli.insert(gt_new_order_sql["update_gtgj_new_order_daily"], tmp_tuple)

        start_date = DateUtil.add_days(start_date, -1)


def mysql_his():
    start_date = DateUtil.date2str(datetime.date(2016, 7, 11))
    end_date = DateUtil.date2str(datetime.date(2016, 7, 12))
    dto = []
    for i in xrange(6):
        dto.append(start_date)
        dto.append(end_date)

    query_data = DBCli().gt_cli.query_all(gt_new_order_sql["gt_neworder_daily"], dto)
    print query_data
    # DBCli().targetdb_cli.batch_insert(gt_new_order_sql["update_gtgj_new_order_daily"], query_data)


def query_gt():

    uids = [
1215805947403328,
]

    conn = Connection('221.235.53.169', 27017)
    db = conn["gtgj"]
    coll_order = db["sub_order"]
    # new_uids = [str(uid) for uid in uids]

    start_date = datetime.date(2016, 1, 1)
    end_date = datetime.date(2016, 9, 30)


    sql = """
        select userid, gt_user_name, uid, date_format(create_time, '%%Y/%%m/%%d'), extend_col1,
        real_name, card from account_gtgj where userid = %s
    """
    # q_uids = DBCli().gt_cli.query_all(sql, dto)

    for uid in uids:
        u_dto = [str(uid)]
        q_u = DBCli().gt_cli.query_one(sql, u_dto)
        u = list(q_u)

        tmp_start_date = int(DateUtil.date2str(start_date, "%Y%m%d"))
        tmp_end_date = int(DateUtil.date2str(end_date, "%Y%m%d"))
        result = coll_order.find({"uid": u[2], "order_date": {"$gte": tmp_start_date, "$lte": tmp_end_date},
                                  "status": {"$nin": ['取消订单', '已退票']}})

        card = u[6]
        sex = None
        age = None
        if card:
            if len(card) > 15:
                if int(card[16]) % 2 == 0:
                    sex = "female"
                else:
                    sex = "male"

                age = card[6:10]
                age = str(2016 - int(age))

        zgp_amount = 0.0
        zgp_num = 0
        zg_gd_num = 0
        dg_amount = 0.0
        dg_num = 0
        dg_gd_num = 0

        for info in result:
            if card == info["card_no"]:
                zgp_amount += float(info["price"])
                zgp_num += 1
                if info["seat_name"] in [u'一等座', u'商务座', u'特等座', u'软卧', u'一等软座']:
                    zg_gd_num += 1
            elif card != info["card_no"]:
                dg_amount += float(info["price"])
                dg_num += 1
                if info["seat_name"] in [u'一等座', u'商务座', u'特等座', u'软卧', u'一等软座']:
                    dg_gd_num += 1
        u.append(sex)
        u.append(age)
        u.append(zgp_amount)
        u.append(zgp_num)
        u.append(zg_gd_num)
        u.append(dg_amount)
        u.append(dg_num)
        u.append(dg_gd_num)
        u.pop(2)
        print u
        insert_sql = """
            insert into gt_test (uid, phone, create_date, city, real_name, card, sex, age, zgp_amount, zgp_num, zg_gd_num, dg_amount,
            dg_num, dg_gd_num) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        DBCli().targetdb_cli.insert(insert_sql, u)


def get_hb():
    uids = [
10190867,
]

    # new_uids = [str(u) for u in uids]
    # sql = """
    #     select uc.phoneid, pu.phone, pu.name, date_format(createtime, '%%Y/%%m/%%d'), uc.id_number from phone_user pu
    #     left join USED_CREDENTIAL uc on pu.phoneid=uc.phoneid
    #     where pu.phoneid in %s
    # """

    sql = """
        select pu.phoneid, pu.phone, pu.name, date_format(createtime, '%%Y/%%m/%%d')
        from phone_user pu
        where pu.phoneid = %s
    """

    card_sql = """
        select id_number from USED_CREDENTIAL where phoneid = %s
    """

    order_sql = """
        select orderid from TICKET_ORDER
        where createtime>='2016-01-01 00:00:00'
        and createtime <= '2016-09-30 00:00:00'
        and ORDERSTATUE not in (2,12,21,51,75)
        and phoneid = %s
    """

    ticket_sql = """
        select outpayprice, passengeridcardno, BASECABIN from TICKET_ORDERDETAIL
        where orderid in %s
    """

    insert_sql = """
        insert into hb_test (uid, phone, real_name, create_date, card, age, sex, city, zgp_amount, zgp_num, zg_gd_num, dg_amount,
        dg_num, dg_gd_num) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for u in uids:
        dto = [str(u)]
        query_data = DBCli().sourcedb_cli.query_one(sql, dto)

        result = []

        req_params = {
            "appkey": "10003",
            "sign": "b59bc3ef6191eb9f747dd4e83c99f2a4",
            "format": "json",
            "app": "phone.get"
        }
        url = "http://api.k780.com:88/"

        zgp_amount = 0.0
        zgp_num = 0
        zg_gd_num = 0
        dg_amount = 0.0
        dg_num = 0
        dg_gd_num = 0
        sex = None
        age = None
        card = None
        phone_city = None
        if query_data:
            query_data = list(query_data)
            phoneid = query_data[0]

            card_dto = [phoneid]
            card = DBCli().sourcedb_cli.query_one(card_sql, card_dto)

            phone = query_data[1]

            req_params["phone"] = phone
            phone_info = requests.get(url, params=req_params)

            phone_city = phone_info.json()

            if phone_city.get("result"):
                phone_city = (phone_city["result"]).get("att", None)
            else:
                phone_city = None
            sex = None
            age = None

            if not card:
                card = None
            else:
                card = card[0]

                if len(card) > 15:
                    if int(card[16]) % 2 == 0:
                        sex = "female"
                    else:
                        sex = "male"

                    age = card[6:10]
                    age = str(2016 - int(age))

            order_dto = [phoneid]
            order = DBCli().sourcedb_cli.query_all(order_sql, order_dto)
            if order:
                # continue
                order = list(order)
                # order = [o[0] for o in order]
                ticket_dto = [order]
                ticket = DBCli().sourcedb_cli.query_all(ticket_sql, ticket_dto)

                for t in ticket:
                    amount = t[0]
                    cardno = t[1]
                    basecabin = t[2]
                    if card == cardno:
                        zgp_amount += float(amount)
                        zgp_num += 1
                        if basecabin in ["F", "P", "C", "J"]:
                            zg_gd_num += 1
                    elif card != cardno:
                        dg_amount += float(amount)
                        dg_num += 1
                        if basecabin in ["F", "P", "C", "J"]:
                            dg_gd_num += 1
        else:
            query_data = [str(u), None, None]
        query_data.append(card)
        query_data.append(age)
        query_data.append(sex)
        query_data.append(phone_city)
        query_data.append(zgp_amount)
        query_data.append(zgp_num)
        query_data.append(zg_gd_num)
        query_data.append(dg_amount)
        query_data.append(dg_num)
        query_data.append(dg_gd_num)
        # result.append(query_data)

        DBCli().targetdb_cli.insert(insert_sql, query_data)


def update_hb_city():
    sql = """
        select uid, phone from hb_test where city is null
    """
    update_sql = """
        update hb_test set city=%s where uid=%s
    """
    phone = DBCli().targetdb_cli.query_all(sql)
    # req_params = {
    #     "appkey": "10003",
    #     "sign": "b59bc3ef6191eb9f747dd4e83c99f2a4",
    #     "format": "json",
    #     "app": "phone.get"
    # }
    # url = "http://api.k780.com:88/"
    url = "http://cx.shouji.360.cn/phonearea.php"
    req_params = {}
    for p in phone:
        uid = p[0]
        phone = p[1]
        req_params["number"] = phone
        phone_info = requests.get(url, params=req_params)

        phone_city = phone_info.json()
        print phone_city
        if phone_city.get("code") == 0:
            phone_city = (phone_city["data"]).get("province", None) + "," + (phone_city["data"]).get("city", None)
        else:
            phone_city = None

        update_dto = [phone_city, uid]
        DBCli().targetdb_cli.insert(update_sql, update_dto)


if __name__ == "__main__":
    # mysql_his()
    # mongo_his()
    query_gt()
    # get_hb()
    # update_hb_city()