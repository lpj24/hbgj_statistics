# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from collections import defaultdict


def update_hbgj_newconsumers_type_daily(days=0):
    """更新航班管家各种类别的新增消费(舱位 国内外 折扣), hbgj_newconsumers_type_daily"""
    start_date = DateUtil.get_date_before_days(int(days))
    end_date = DateUtil.get_date_after_days(1 - int(days))
    dto = [start_date, end_date, start_date]
    ticket_sql = """
            select TICKET_ORDER.PHONEID, TICKET_ORDERDETAIL.BASECABIN BASECABIN,
            TICKET_ORDERDETAIL.DISCOUNT, TICKET_ORDER.INTFLAG, TICKET_ORDER.ORDERID from TICKET_ORDERDETAIL
            LEFT JOIN TICKET_ORDER ON TICKET_ORDERDETAIL.ORDERID = TICKET_ORDER.ORDERID
            WHERE TICKET_ORDER.ORDERSTATUE not in (0, 1, 11, 12, 2, 21, 3, 31)
            AND TICKET_ORDER.CREATETIME >= %s
            AND TICKET_ORDER.CREATETIME < %s
            AND IFNULL(TICKET_ORDERDETAIL.`LINKTYPE`, 0) != 2
            and TICKET_ORDER.PHONEID not in
            (
            select TICKET_ORDER.PHONEID from TICKET_ORDER
            WHERE TICKET_ORDER.ORDERSTATUE not in (0, 1, 11, 12, 2, 21, 3, 31)
            AND TICKET_ORDER.CREATETIME < %s
            AND IFNULL(TICKET_ORDERDETAIL.`LINKTYPE`, 0) != 2
            )
            ORDER BY TICKET_ORDER.PHONEID, TICKET_ORDER.CREATETIME
    """

    insert_sql = """
        insert into hbgj_newconsumers_type_daily
        (s_day, y_basecabin_consumers, no_y_basecabin_consumers,
        y_basecabin_ticket_num, no_y_basecabin_ticket_num,
        discount_one_consumers, discount_two_consumers, discount_three_consumers,
        discount_one_ticket_num, discount_two_ticket_num, discount_three_ticket_num,
        inter_consumers, inland_consumers, inter_ticket_num, inland_ticket_num,
        createtime, updatetime)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
    """

    query_all_data = DBCli().sourcedb_cli.query_all(ticket_sql, dto)
    user_casecabin = {}

    discount_user = defaultdict(list)
    discount_ticket = defaultdict(list)
    inter_user = []
    inland_user = []

    inter_ticket = []
    inland_ticket = []

    basecabin_user = defaultdict(list)
    basecabin_ticket = defaultdict(list)

    for value in query_all_data:
        phoneid, basecabin, discount, intflag, orderid = value
        if 0.7 <= float(discount) <= 1:
            if phoneid not in discount_user["0.5_0.7"] + discount_user["0_0.5"]:
                discount_user["0.7_1"].append(phoneid)
            discount_ticket["0.7_1"].append(phoneid)
        elif 0.5 <= float(discount) < 0.7:
            if phoneid not in discount_user["0.7_1"] + discount_user["0_0.5"]:
                discount_user["0.5_0.7"].append(phoneid)
            discount_ticket["0.5_0.7"].append(phoneid)
        elif 0 <= float(discount) < 0.5:
            if phoneid not in discount_user["0.7_1"] + discount_user["0.5_0.7"]:
                discount_user["0_0.5"].append(phoneid)
            discount_ticket["0_0.5"].append(phoneid)

        if intflag == 1:
            if phoneid not in inland_user:
                inter_user.append(phoneid)
            inter_ticket.append(phoneid)
        elif intflag == 0:
            if phoneid not in inter_user:
                inland_user.append(phoneid)
            inland_ticket.append(phoneid)

        basecabin_ticket[basecabin].append(phoneid)
        if phoneid not in user_casecabin or (phoneid in user_casecabin and user_casecabin[phoneid] == basecabin):
            basecabin_user[basecabin].append(phoneid)
            user_casecabin[phoneid] = basecabin

    y_user_num = 0
    y_ticket_num = 0
    n_y_user_num = 0
    n_y_ticket_num = 0
    for basecabin_k, basecabin_v in basecabin_user.items():
        if basecabin_k != 'Y':
            n_y_user_num += len(set(basecabin_v))
        else:
            y_user_num += len(set(basecabin_v))

    for k, v in basecabin_ticket.items():
        if k != 'Y':
            n_y_ticket_num += len(v)
        else:
            y_ticket_num += len(v)
    inter_user_num = len(set(inter_user))
    inter_ticket_num = len(inter_ticket)

    inland_user_num = len(set(inland_user))
    inland_ticket_num = len(inland_ticket)

    one_discount_user_num = len(set(discount_user["0.7_1"]))
    one_discount_ticket_num = len(discount_ticket["0.7_1"])

    two_discount_user_num = len(set(discount_user["0.5_0.7"]))
    two_discount_ticket_num = len(discount_ticket["0.5_0.7"])

    three_discount_user_num = len(set(discount_user["0_0.5"]))
    three_discount_ticket_num = len(discount_ticket["0_0.5"])
    hbgj_newconsumers_data = [DateUtil.date2str(start_date, '%Y-%m-%d'), str(y_user_num), str(n_y_user_num),
                              str(y_ticket_num), str(n_y_ticket_num), str(one_discount_user_num),
                              str(two_discount_user_num), str(three_discount_user_num), str(one_discount_ticket_num),
                              str(two_discount_ticket_num), str(three_discount_ticket_num), str(inter_user_num),
                              str(inland_user_num), str(inter_ticket_num), str(inland_ticket_num)]
    DBCli().targetdb_cli.insert(insert_sql, hbgj_newconsumers_data)

    pass


def update_new_register_user_daily(days=0):
    """更新新增注册用户, hbgj_new_register_user_daily"""
    register_sql = """
        select
        A.s_day,
        sum(case when A.fromsrc = 'gtgj' then A.num end) gt_num,
        sum(case when A.fromsrc = 'hbgj' or A.fromsrc not in ('gtgj', 'weixin') or A.fromsrc is null then A.num end) hb_num,
        sum(case when A.fromsrc = 'weixin' then A.num end) weixin_num
         from (
        select DATE_FORMAT(CREATETIME, '%%Y-%%m-%%d') s_day, fromsrc,count(1) num
        from phone_user
        where CREATETIME>=%s
        and CREATETIME<%s
        GROUP BY fromsrc, s_day
        order by num desc) A GROUP BY A.s_day;
    """

    insert_sql = """
        insert into hbgj_new_register_user_daily (s_day, gt_user_num, hb_user_num,
        weixin_user_num, createtime, updatetime)
        values (%s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        gt_user_num = values(gt_user_num),
        hb_user_num = values(hb_user_num),
        weixin_user_num = values(weixin_user_num)
    """
    start_date = DateUtil.get_date_before_days(int(days))
    end_date = DateUtil.get_date_after_days(1 - int(days))
    dto = [start_date, end_date]
    register_data = DBCli().sourcedb_cli.query_one(register_sql, dto)
    DBCli().targetdb_cli.insert(insert_sql, register_data)
    pass


def update_hbgj_inter_inland_consumers_daily(days=0):
    """更新航班国际国内消费用户, hbgj_inter_inland_consumers_daily"""
    inter_inland_sql = """
        SELECT DATE_FORMAT(TICKET_ORDER.CREATETIME,'%%Y-%%m-%%d') s_day,
        count(DISTINCT TICKET_ORDER.PHONEID) consumers,
        count(DISTINCT case when TICKET_ORDER.INTFLAG=1 then TICKET_ORDER.PHONEID END) inter_consumers_num,
        count(DISTINCT case when TICKET_ORDER.INTFLAG=0 then TICKET_ORDER.PHONEID END) inland_consumers_num
        FROM TICKET_ORDERDETAIL
        join TICKET_ORDER
        ON TICKET_ORDER.ORDERID = TICKET_ORDERDETAIL.ORDERID
        where TICKET_ORDER.ORDERSTATUE not in (0, 1, 11, 12, 2, 21, 3, 31)
        AND IFNULL(TICKET_ORDERDETAIL.`LINKTYPE`, 0) != 2
        and  TICKET_ORDER.CREATETIME >= %s
        and  TICKET_ORDER.CREATETIME < %s
        GROUP BY s_day;
    """

    insert_sql = """
        insert into hbgj_inter_inland_consumers_daily (s_day, total_consumers,
        inter_consumers, inland_consumers, createtime, updatetime)
        values (%s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        total_consumers = values(total_consumers),
        inter_consumers = values(inter_consumers),
        inland_consumers = values(inland_consumers)
    """
    start_date = DateUtil.get_date_before_days(int(days))
    end_date = DateUtil.get_date_after_days(1 - int(days))
    dto = [start_date, end_date]
    inter_inland_data = DBCli().sourcedb_cli.query_one(inter_inland_sql, dto)
    DBCli().targetdb_cli.insert(insert_sql, inter_inland_data)
    pass


if __name__ == "__main__":
    i = 3
    while i >= 1:
        update_hbgj_inter_inland_consumers_daily(i)
        i -= 1
    # update_new_register_user_daily(1)

    # update_hbgj_inter_inland_consumers_daily(1)