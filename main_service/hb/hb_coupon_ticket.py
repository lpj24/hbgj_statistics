# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from sql.hb_sqlHandlers import coupon_sql


def update_hbgj_coupon_tickt(days=0):
    """航班机票优惠券发放与使用情况, coupon_hbgj_ticket"""
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)

    dto = [start_date, end_date]
    use_coupon_data = DBCli().sourcedb_cli.query_one(coupon_sql["hbgj_use_coupon_sql"], dto)
    issue_coupon_data = DBCli().hb_source_account_cli.query_one(coupon_sql["hbgj_issue_coupon_sql"], dto)
    insert_data = issue_coupon_data + use_coupon_data
    DBCli().targetdb_cli.insert(coupon_sql["insert_hbgj_coupon_sql"], insert_data)
    pass


def update_gt_coupon_daily(days=0):
    """高铁优惠券发送与使用数量, coupon_gtgj_ticket"""
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)

    dto = [start_date, end_date, start_date, end_date]
    gtgj_coupon_data = DBCli().gt_cli.query_one(coupon_sql["gtgj_use_issue_coupon_sql"], dto)
    DBCli().targetdb_cli.insert(coupon_sql["insert_gtgj_coupon_sql"], gtgj_coupon_data)
    pass


def update_huoli_car_coupon_daily(days=0):
    """专车优惠券发放与使用数量, coupon_huoli_car"""
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)
    dto = [start_date, end_date]
    use_coupon_data = DBCli().sourcedb_cli.query_one(coupon_sql["huoli_car_use_coupon_sql"], dto)
    issue_coupon_data = DBCli().hb_source_account_cli.query_one(coupon_sql["huoli_car_issue_coupon_sql"], dto)
    insert_data = issue_coupon_data + use_coupon_data
    DBCli().targetdb_cli.insert(coupon_sql["insert_huoli_car_sql"], insert_data)
    pass


def update_huoli_hotel_coupon_daily(days=0):
    """酒店优惠券发放与使用数量, coupon_huoli_hotel"""
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)

    dto = [start_date, end_date]
    use_coupon_data = DBCli().sourcedb_cli.query_one(coupon_sql["huoli_hotel_use_coupon_sql"], dto)
    use_coupon_data = use_coupon_data if use_coupon_data[0] else (0, 0, 0, 0)
    issue_coupon_data = DBCli().hb_source_account_cli.query_one(coupon_sql["huoli_hotel_issue_coupon_sql"], dto)
    issue_coupon_data = issue_coupon_data if issue_coupon_data[0] else (0, 0, 0)
    insert_data = (DateUtil.date2str(start_date, '%Y-%m-%d'), ) + issue_coupon_data + use_coupon_data
    DBCli().targetdb_cli.insert(coupon_sql["insert_huoli_hotel_sql"], insert_data)
    pass


def update_common_coupon_daily(days=0):
    """普通优惠券发送数量, coupon_common"""
    start_date = DateUtil.get_date_before_days(days * 3)
    end_date = DateUtil.get_date_after_days(1 - days)
    dto = [start_date, end_date]
    common_coupon_data = DBCli().hb_source_account_cli.query_all(coupon_sql["common_coupon_sql"], dto)
    DBCli().targetdb_cli.batch_insert(coupon_sql["insert_common_coupon_sql"], common_coupon_data)
    pass


def update_hb_coupon_use_detail_daily(days=0):
    """航班优惠券使用详情, coupon_hbgj_ticket_use_detail coupon_hbgj_ticket_use_detail_client"""
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)

    dto = [start_date, end_date]
    use_detail_coupon_data = DBCli().sourcedb_cli.query_all(coupon_sql["hbdj_use_detail_sql"], dto)
    DBCli().targetdb_cli.batch_insert(coupon_sql["insert_hbgj_use_detail_sql"], use_detail_coupon_data)
    use_detail_noclient_coupon_data = DBCli().sourcedb_cli.query_all(coupon_sql["hbdj_use_detail_noclient_sql"], dto)
    DBCli().targetdb_cli.batch_insert(coupon_sql["insert_hbgj_use_detail_noclient_sql"], use_detail_noclient_coupon_data)
    pass


def update_coupon_use_detail_daily(days=0):
    """优惠券发放详情, coupon_issue_detail"""
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)
    dto = [start_date, end_date]
    use_detail_coupon_data = DBCli().hb_source_account_cli.query_all(coupon_sql["coupon_issue_detail_sql"], dto)
    DBCli().targetdb_cli.batch_insert(coupon_sql["insert_coupon_issue_detail_sql"], use_detail_coupon_data)
    pass


def update_car_use_detail_daily(days=0):
    """专车优惠券使用详情, coupon_huoli_car_use_detail"""
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)
    dto = [start_date, end_date]
    use_detail_coupon_data = DBCli().sourcedb_cli.query_all(coupon_sql["huoli_car_coupon_detail_sql"], dto)
    DBCli().targetdb_cli.batch_insert(coupon_sql["insert_coupon_car_use_detail_sql"], use_detail_coupon_data)
    pass


def update_hotel_use_detail_daily(days=0):
    """酒店优惠券使用详情, coupon_huoli_hotel_use_detail"""
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)
    dto = [start_date, end_date]
    use_detail_coupon_data = DBCli().sourcedb_cli.query_all(coupon_sql["huoli_hotel_use_detail_sql"], dto)
    DBCli().targetdb_cli.batch_insert(coupon_sql["insert_huoli_hotel_use_detail_sql"], use_detail_coupon_data)
    pass


def update_gtgj_use_issue_detail_daily(days=0):
    """高铁优惠券使用详情, coupon_gtgj_ticket_use_detail"""
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)
    dto = [start_date, end_date]
    issue_detail_coupon_data = DBCli().gt_cli.query_all(coupon_sql["gtgj_coupon_issue_detail_sql"], dto)
    DBCli().targetdb_cli.batch_insert(coupon_sql["insert_gtgj_coupon_issue_detail_sql"], issue_detail_coupon_data)

    use_detail_coupon_data = DBCli().gt_cli.query_all(coupon_sql["gtgj_coupon_use_detail_sql"], dto)
    DBCli().targetdb_cli.batch_insert(coupon_sql["insert_gtgj_coupon_use_detail_sql"], use_detail_coupon_data)
    pass


def update_coupon_use_detail_daily_his(days=0):
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)
    dto = [start_date, end_date]
    use_detail_coupon_data = DBCli().hb_source_account_cli.query_all(coupon_sql["coupon_issue_detail_sql"], dto)
    DBCli().targetdb_cli.batch_insert(coupon_sql["insert_coupon_issue_detail_sql"], use_detail_coupon_data)


def update_hb_coupon_use_detail_daily_his(days=0):
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)

    dto = [start_date, end_date]
    use_detail_coupon_data = DBCli().sourcedb_cli.query_all(coupon_sql["hbdj_use_detail_sql"], dto)
    DBCli().targetdb_cli.batch_insert(coupon_sql["insert_hbgj_use_detail_sql"], use_detail_coupon_data)


def update_common_coupon_his():
    end_date = DateUtil.get_date_after_days(0)
    dto = [end_date]
    common_coupon_data = DBCli().hb_source_account_cli.query_all(coupon_sql["common_coupon_sql"], dto)
    DBCli().targetdb_cli.batch_insert(coupon_sql["insert_common_coupon_sql"], common_coupon_data)


def update_huoli_hotel_coupon_his():
    import datetime
    query_date = datetime.date(2017, 2, 16)
    huoli_hotel_use_coupon_sql = """
        select
        sum(case when TRADE_TYPE=1 THEN 1 ELSE 0 END) use_coupon_count_in,
        sum(case when TRADE_TYPE=1 THEN price ELSE 0 END) use_coupon_amount_in,
        sum(case when TRADE_TYPE=4 THEN 1 ELSE 0 END) use_coupon_count_return,
        sum(case when TRADE_TYPE=4 THEN price ELSE 0 END) use_coupon_amount_return,
        DATE_FORMAT(createtime,'%%Y-%%m-%%d') s_day
        from TRADE_RECORD
        where productid=36
        and PAYSOURCE LIKE '%%coupon%%'
        and createtime<%s
        group by s_day
    """

    huoli_hotel_issue_coupon_sql = """
        select DATE_FORMAT(C.createtime, '%%Y-%%m-%%d') s_day,
        sum(case when CL.amount>1 then 1 else 0 end) issue_coupon_count,
        sum(case when CL.amount>1 then C.amount else 0 end) issue_coupon_amount,
        sum(case when CL.amount=1.0 then 1 else 0 end) issue_discount_coupon_count
        from coupon C
        left join coupon_list CL on C.coupon_id = CL.id
        where C.bindtype=9
        and C.createtime<%s
        group by s_day
        order by s_day
    """

    insert_huoli_hotel_sql = """
        insert into coupon_huoli_hotel (s_day, issue_coupon_count, issue_coupon_amount, issue_discount_coupon_count,
        createtime, updatetime) values (%s, %s, %s, %s, now(), now())
    """

    update_sql = """
        update coupon_huoli_hotel set use_coupon_count_in=%s, use_coupon_amount_in=%s,
        use_coupon_count_return=%s, use_coupon_amount_return=%s where s_day=%s
    """

    issue_coupon_data = DBCli().hb_source_account_cli.query_all(huoli_hotel_issue_coupon_sql, [query_date])
    DBCli().targetdb_cli.batch_insert(insert_huoli_hotel_sql, issue_coupon_data)

    use_coupon_data = DBCli().sourcedb_cli.query_all(huoli_hotel_use_coupon_sql, [query_date])
    DBCli().targetdb_cli.batch_insert(update_sql, use_coupon_data)


def update_huoli_car_coupon_his():
    import datetime
    query_date = datetime.date(2017, 2, 15)
    huoli_car_use_coupon_sql = """
        select
        sum(case when TRADE_TYPE=1 THEN 1 ELSE 0 END) use_coupon_count_in,
        sum(case when TRADE_TYPE=1 THEN price ELSE 0 END) use_coupon_amount_in,
        sum(case when TRADE_TYPE=4 THEN 1 ELSE 0 END) use_coupon_count_return,
        sum(case when TRADE_TYPE=4 THEN price ELSE 0 END) use_coupon_amount_return,
        DATE_FORMAT(createtime,'%%Y-%%m-%%d') s_day
        from TRADE_RECORD
        where productid=7
        and PAYSOURCE LIKE '%%coupon%%'
        and createtime<%s
        group by s_day
    """

    huoli_car_issue_coupon_sql = """
        select DATE_FORMAT(C.createtime, '%%Y-%%m-%%d') s_day,
        sum(case when CL.amount>1 then 1 else 0 end) issue_coupon_count,
        sum(case when CL.amount>1 then C.amount else 0 end) issue_coupon_amount,
        sum(case when CL.amount=1.0 then 1 else 0 end) issue_discount_coupon_count
        from coupon C
        left join coupon_list CL on C.coupon_id = CL.id
        where C.bindtype=3
        and C.createtime<%s
        group by s_day
        order by s_day
    """

    insert_huoli_car_sql = """
        insert into coupon_huoli_car (s_day, issue_coupon_count, issue_coupon_amount, issue_discount_coupon_count,
        createtime, updatetime) values (%s, %s, %s, %s, now(), now())
    """

    update_sql = """
        update coupon_huoli_car set use_coupon_count_in=%s, use_coupon_amount_in=%s,
        use_coupon_count_return=%s, use_coupon_amount_return=%s where s_day=%s
    """

    issue_coupon_data = DBCli().hb_source_account_cli.query_all(huoli_car_issue_coupon_sql, [query_date])
    DBCli().targetdb_cli.batch_insert(insert_huoli_car_sql, issue_coupon_data)

    use_coupon_data = DBCli().sourcedb_cli.query_all(huoli_car_use_coupon_sql, [query_date])
    DBCli().targetdb_cli.batch_insert(update_sql, use_coupon_data)


def update_gt_coupon_daily_his():
    import datetime
    his_date = datetime.date(2017, 2, 22)
    his_data = DBCli().gt_cli.query_all(coupon_sql["gtgj_use_issue_coupon_sql"], [his_date, his_date])
    DBCli().targetdb_cli.batch_insert(coupon_sql["insert_gtgj_coupon_sql"], his_data)


def update_hb_coupon_his():
    issue_sql = """
        select DATE_FORMAT(C.createtime,'%Y-%m-%d') s_day,
        sum(case when CL.amount>1 then 1 else 0 end) issue_coupon_count,
        sum(case when CL.amount>1 then C.amount else 0 end) issue_coupon_amount,
        sum(case when CL.amount=1.0 then 1 else 0 end) issue_discount_coupon_count
        from coupon C
        left join coupon_list CL on C.coupon_id = CL.id
        where C.bindtype=1
        and C.createtime<'2017-02-15'
        group by s_day
        order by s_day
    """

    insert_sql = """
        insert into coupon_hbgj_ticket (s_day, issue_coupon_count, issue_coupon_amount, issue_discount_coupon_count,
        createtime, updatetime) values (%s, %s, %s, %s, now(), now())

    """

    issue_coupon_data = DBCli().hb_source_account_cli.query_all(issue_sql)
    DBCli().targetdb_cli.batch_insert(insert_sql, issue_coupon_data)
    use_sql = """
        select
        sum(case when TRADE_TYPE=1 THEN 1 ELSE 0 END) use_coupon_count_in,
        sum(case when TRADE_TYPE=1 THEN price ELSE 0 END) use_coupon_amount_in,
        sum(case when TRADE_TYPE=4 THEN 1 ELSE 0 END) use_coupon_count_return,
        sum(case when TRADE_TYPE=4 THEN price ELSE 0 END) use_coupon_amount_return,
        DATE_FORMAT(createtime,'%Y-%m-%d') s_day
        from TRADE_RECORD
        where productid=0
        and PAYSOURCE LIKE '%coupon%'
        and createtime<'2017-02-15'
        group by s_day
        order by s_day
    """

    use_coupon_data = DBCli().sourcedb_cli.query_all(use_sql)
    update_sql = """
        update coupon_hbgj_ticket set use_coupon_count_in=%s, use_coupon_amount_in=%s,
        use_coupon_count_return=%s, use_coupon_amount_return=%s where s_day = %s
    """
    DBCli().targetdb_cli.batch_insert(update_sql, use_coupon_data)


def update_profit_huoli_fmall_cost(days=0):
    """商城优惠券使用, profit_huoli_fmall_cost"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days*3))
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - days))
    dto = [start_date, end_date]

    fmall_coupon_sql = """
        select distinct TRADE_TIME s_day,
        sum(case when (AMOUNT_TYPE =1 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST) in ('21', '22')
        and (PRODUCT=COST OR COST IS NULL)
        and TRADE_CHANNEL='coupon') then amount else 0 end) coupon_in,
        sum(case when (AMOUNT_TYPE =4 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST) in ('21', '22')
        and (PRODUCT=COST OR COST IS NULL)
        and TRADE_CHANNEL='coupon') then amount else 0 end) coupon_return,
        sum(case when (AMOUNT_TYPE =1 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST) in ('21', '22')
        and PRODUCT!=COST
        and TRADE_CHANNEL='coupon') then amount else 0 end) else_coupon_in,
        sum(case when (AMOUNT_TYPE =4 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST) in ('21', '22')
        and PRODUCT!=COST
        and TRADE_CHANNEL='coupon') then amount else 0 end) else_coupon_return
        from PAY_COST_INFO where TRADE_TIME>=%s and TRADE_TIME<%s
        group by TRADE_TIME
    """

    insert_sql = """
        insert into profit_huoli_fmall_cost (s_day, coupon_in, coupon_return,
        else_coupon_in, else_coupon_return, createtime, updatetime)
        values (%s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        coupon_in = values(coupon_in),
        coupon_return = values(coupon_return),
        else_coupon_in = values(else_coupon_in),
        else_coupon_return = values(else_coupon_return)
    """
    fmall_data = DBCli().pay_cost_cli.query_all(fmall_coupon_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, fmall_data)
    pass


def update_profit_huoli_buy_cost(days=0):
    """卖好货优惠券使用, profit_huoli_buy_cost"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days*3))
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - days))
    dto = [start_date, end_date]

    buy_coupon_sql = """
        select distinct TRADE_TIME s_day,
        sum(case when (AMOUNT_TYPE =1 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST) = '48'
        and (PRODUCT=COST OR COST IS NULL)
        and TRADE_CHANNEL='coupon') then amount else 0 end) coupon_in,
        sum(case when (AMOUNT_TYPE =4 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST) = '48'
        and (PRODUCT=COST OR COST IS NULL)
        and TRADE_CHANNEL='coupon') then amount else 0 end) coupon_return,
        sum(case when (AMOUNT_TYPE =1 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST) = '48'
        and PRODUCT!=COST
        and TRADE_CHANNEL='coupon') then amount else 0 end) else_coupon_in,
        sum(case when (AMOUNT_TYPE =4 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST) = '48'
        and PRODUCT!=COST
        and TRADE_CHANNEL='coupon') then amount else 0 end) else_coupon_return
        from PAY_COST_INFO where TRADE_TIME>=%s and TRADE_TIME<%s
        group by TRADE_TIME
    """

    insert_sql = """
        insert into profit_huoli_buy_cost (s_day, coupon_in, coupon_return, else_coupon_in, else_coupon_return,
        createtime, updatetime)
        values (%s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        coupon_in = values(coupon_in),
        coupon_return = values(coupon_return),
        else_coupon_in = values(else_coupon_in),
        else_coupon_return = values(else_coupon_return)
    """
    buy_data = DBCli().pay_cost_cli.query_all(buy_coupon_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, buy_data)
    pass


def update_coupon_list(days=1):
    """同步skyhotl数据库中coupon_list表数据, coupon_list"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 1), '%Y-%m-%d')
    sql = """
        select id, amount, manager, start_time, end_time, type,
        createtime, bindtype, rules, txt, coupon_type, coupon_name,
        check_url, check_channeltype, turn_flag, costid
        from coupon_list where createtime >= %s
    """
    insert_sql = """
        insert into coupon_list (id, amount, manager, start_time, end_time, type,
        createtime, bindtype, rules, txt, coupon_type, coupon_name,
        check_url, check_channeltype, turn_flag, costid)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    query_data = DBCli().hb_sky_account_cli.query_all(sql, [start_date])
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)
    pass

if __name__ == "__main__":
    # update_common_coupon_daily(1)
    # update_huoli_hotel_coupon_daily(1)
    # update_huoli_car_coupon_daily(1)
    # update_hbgj_coupon_tickt(4)
    # update_huoli_hotel_coupon_daily(4)
    update_profit_huoli_fmall_cost(1)