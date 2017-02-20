# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from sql.hb_sqlHandlers import coupon_sql


def update_hbgj_coupon_tickt(days=0):
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)

    dto = [start_date, end_date]
    use_coupon_data = DBCli().sourcedb_cli.queryOne(coupon_sql["hbgj_use_coupon_sql"], dto)
    issue_coupon_data = DBCli().hb_source_account_cli.queryOne(coupon_sql["hbgj_issue_coupon_sql"], dto)
    insert_data = issue_coupon_data + use_coupon_data
    DBCli().targetdb_cli.insert(coupon_sql["insert_hbgj_coupon_sql"], insert_data)


def update_gt_coupon_daily(days=0):
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)

    dto = [start_date, end_date, start_date, end_date]
    gtgj_coupon_data = DBCli().gt_cli.queryOne(coupon_sql["gtgj_use_issue_coupon_sql"], dto)
    DBCli().targetdb_cli.insert(coupon_sql["insert_gtgj_coupon_sql"], gtgj_coupon_data)


def update_huoli_car_coupon_daily(days=0):
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)

    dto = [start_date, end_date]
    use_coupon_data = DBCli().sourcedb_cli.queryOne(coupon_sql["huoli_car_use_coupon_sql"], dto)
    issue_coupon_data = DBCli().hb_source_account_cli.queryOne(coupon_sql["huoli_car_issue_coupon_sql"], dto)
    insert_data = issue_coupon_data + use_coupon_data
    DBCli().targetdb_cli.insert(coupon_sql["insert_huoli_car_sql"], insert_data)


def update_huoli_hotel_coupon_daily(days=0):
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)

    dto = [start_date, end_date]
    use_coupon_data = DBCli().sourcedb_cli.queryOne(coupon_sql["huoli_hotel_use_coupon_sql"], dto)
    use_coupon_data = use_coupon_data if use_coupon_data[0] else (0, 0, 0, 0)
    issue_coupon_data = DBCli().hb_source_account_cli.queryOne(coupon_sql["huoli_hotel_issue_coupon_sql"], dto)
    issue_coupon_data = issue_coupon_data if issue_coupon_data[0] else (0, 0, 0)
    insert_data = (DateUtil.date2str(start_date, '%Y-%m-%d'), ) + issue_coupon_data + use_coupon_data
    DBCli().targetdb_cli.insert(coupon_sql["insert_huoli_hotel_sql"], insert_data)


def update_common_coupon_daily(days=0):
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)

    dto = [start_date, end_date]
    common_coupon_data = DBCli().hb_source_account_cli.queryOne(coupon_sql["common_coupon_sql"], dto)
    DBCli().targetdb_cli.insert(coupon_sql["insert_common_coupon_sql"], common_coupon_data)


def update_hb_coupon_use_detail_daily(days=0):
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)

    dto = [start_date, end_date]
    use_detail_coupon_data = DBCli().sourcedb_cli.queryOne(coupon_sql["hbdj_use_detail_sql"], dto)
    DBCli().targetdb_cli.insert(coupon_sql["insert_hbgj_use_detail_sql"], use_detail_coupon_data)


def update_coupon_use_detail_daily(days=0):
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)
    dto = [start_date, end_date]
    use_detail_coupon_data = DBCli().hb_source_account_cli.queryOne(coupon_sql["coupon_issue_detail_sql"], dto)
    DBCli().targetdb_cli.insert(coupon_sql["insert_coupon_issue_detail_sql"], use_detail_coupon_data)


def update_coupon_use_detail_daily_his(days=0):
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)
    dto = [start_date]
    use_detail_coupon_data = DBCli().hb_source_account_cli.queryAll(coupon_sql["coupon_issue_detail_sql"], dto)
    DBCli().targetdb_cli.batchInsert(coupon_sql["insert_coupon_issue_detail_sql"], use_detail_coupon_data)


def update_hb_coupon_use_detail_daily_his(days=0):
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - days)

    dto = [start_date]
    use_detail_coupon_data = DBCli().sourcedb_cli.queryAll(coupon_sql["hbdj_use_detail_sql"], dto)
    DBCli().targetdb_cli.batchInsert(coupon_sql["insert_hbgj_use_detail_sql"], use_detail_coupon_data)


def update_common_coupon_his():
    end_date = DateUtil.get_date_after_days(0)
    dto = [end_date]
    common_coupon_data = DBCli().hb_source_account_cli.queryAll(coupon_sql["common_coupon_sql"], dto)
    DBCli().targetdb_cli.batchInsert(coupon_sql["insert_common_coupon_sql"], common_coupon_data)


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

    issue_coupon_data = DBCli().hb_source_account_cli.queryAll(huoli_hotel_issue_coupon_sql, [query_date])
    DBCli().targetdb_cli.batchInsert(insert_huoli_hotel_sql, issue_coupon_data)

    use_coupon_data = DBCli().sourcedb_cli.queryAll(huoli_hotel_use_coupon_sql, [query_date])
    DBCli().targetdb_cli.batchInsert(update_sql, use_coupon_data)


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

    issue_coupon_data = DBCli().hb_source_account_cli.queryAll(huoli_car_issue_coupon_sql, [query_date])
    DBCli().targetdb_cli.batchInsert(insert_huoli_car_sql, issue_coupon_data)

    use_coupon_data = DBCli().sourcedb_cli.queryAll(huoli_car_use_coupon_sql, [query_date])
    DBCli().targetdb_cli.batchInsert(update_sql, use_coupon_data)


def update_gt_coupon_daily_his():
    import datetime
    his_date = datetime.date(2017, 2, 15)
    his_data = DBCli().gt_cli.queryAll(coupon_sql["gtgj_use_issue_coupon_sql"], [his_date, his_date])
    DBCli().targetdb_cli.batchInsert(coupon_sql["insert_gtgj_coupon_sql"], his_data)


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

    issue_coupon_data = DBCli().hb_source_account_cli.queryAll(issue_sql)
    print issue_coupon_data
    DBCli().targetdb_cli.batchInsert(insert_sql, issue_coupon_data)
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

    use_coupon_data = DBCli().sourcedb_cli.queryAll(use_sql)
    update_sql = """
        update coupon_hbgj_ticket set use_coupon_count_in=%s, use_coupon_amount_in=%s,
        use_coupon_count_return=%s, use_coupon_amount_return=%s where s_day = %s
    """
    DBCli().targetdb_cli.batchInsert(update_sql, use_coupon_data)

if __name__ == "__main__":
    # update_hbgj_coupon_tickt(1)
    # update_hb_coupon_his()
    # update_gt_coupon_daily(1)
    # update_gt_coupon_daily_his()
    # update_huoli_car_coupon_daily(1)
    # update_huoli_car_coupon_his()
    # result = DBCli().oracle_cli.queryOne(sql)
    # import logging
    # logging.warning(result)
    # update_huoli_hotel_coupon_daily(1)
    # update_huoli_hotel_coupon_his()
    # update_common_coupon_daily(1)
    # update_common_coupon_his()
    # update_gt_coupon_daily(1)
    # update_hbgj_coupon_tickt(1)
    # update_huoli_car_coupon_daily(1)
    # update_huoli_hotel_coupon_daily(1)
    # update_common_coupon_daily(1)
    # update_hb_coupon_use_detail_daily(1)
    # update_hb_coupon_use_detail_daily_his()
    # update_coupon_use_detail_daily(1)
    update_coupon_use_detail_daily_his(1)