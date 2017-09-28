# -*- coding: utf-8 -*-
from dbClient.dateutil import DateUtil
from dbClient.db_client import DBCli


def update_huoli_buy_orders_daily(days=0):
    """更新huolibuy的每日订单数, huoli_buy_orders_daily"""
    start_date = DateUtil.get_date_before_days(int(days) * 3)
    end_date = DateUtil.get_date_after_days(1-int(days))
    huoli_order_sql = """
        select DATE_FORMAT(po.createtime, '%%Y-%%m-%%d') s_day, count(DISTINCT po.id),
        sum(po.totalprice) from product_order po
        left join pay on po.pay_id=pay.id
        where pay.paysource!='FREE' and po.account_type='hl'
        and po.status = 30
        and po.pid !=0
        and po.createtime >= %s
        and po.createtime < %s
        GROUP BY s_day
    """

    insert_sql = """
        insert into huoli_buy_orders_daily (s_day, orders_num, orders_amount, createtime, updatetime)
        values (%s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        orders_num = values(orders_num),
        orders_amount = values(orders_amount)
    """
    dto = [start_date, end_date]
    query_data = DBCli().huoli_buy_cli.query_all(huoli_order_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)


def update_huoli_buy_consumers_daily(days=0):
    """更新huolibuy的每日消费用户数, huoli_buy_consumers_daily"""
    start_date = DateUtil.get_date_before_days(int(days) * 3)
    end_date = DateUtil.get_date_after_days(1-int(days))
    huoli_consumers_sql = """
        select DATE_FORMAT(po.createtime, '%%Y-%%m-%%d') s_day, count(DISTINCT po.userid), sum(po.totalprice) from product_order po
        left join pay on po.pay_id=pay.id
        where pay.paysource!='FREE' and po.account_type='hl'
        and po.status = 30
        and po.pid !=0
        and po.createtime >= %s
        and po.createtime < %s
        GROUP BY s_day
    """

    insert_sql = """
        insert into huoli_buy_consumers_daily (s_day, consumers, consumers_amount, createtime, updatetime)
        values (%s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        consumers = values(consumers),
        consumers_amount = values(consumers_amount)
    """
    dto = [start_date, end_date]
    query_data = DBCli().huoli_buy_cli.query_all(huoli_consumers_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)


def update_huoli_buy_consumers_weekly():
    """更新伙力精选消费用户周, huoli_buy_consumers_weekly"""
    start_date, end_date = DateUtil.get_last_week_date()
    huoli_consumers_sql = """
        select %s, count(DISTINCT po.userid),
        sum(po.totalprice) from product_order po
        left join pay on po.pay_id=pay.id
        where pay.paysource!='FREE' and po.account_type='hl'
        and po.status = 30
        and po.pid !=0
        and po.createtime >= %s
        and po.createtime < %s
    """

    insert_sql = """
        insert into huoli_buy_consumers_weekly (s_day, consumers, consumers_amount, createtime, updatetime)
        values (%s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        consumers = values(consumers),
        consumers_amount = values(consumers_amount)
    """
    dto = [start_date, start_date, end_date]
    query_data = DBCli().huoli_buy_cli.query_all(huoli_consumers_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)


def update_huoli_buy_consumers_monthly():
    start_date, end_date = DateUtil.get_last_month_date()
    huoli_consumers_sql = """
        select %s, count(DISTINCT po.userid),
        sum(po.totalprice) from product_order po
        left join pay on po.pay_id=pay.id
        where pay.paysource!='FREE' and po.account_type='hl'
        and po.status = 30
        and po.pid !=0
        and po.createtime >= %s
        and po.createtime < %s
    """

    insert_sql = """
        insert into huoli_buy_consumers_monthly (s_day, consumers, consumers_amount, createtime, updatetime)
        values (%s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        consumers = values(consumers),
        consumers_amount = values(consumers_amount)
    """
    dto = [start_date, start_date, end_date]
    query_data = DBCli().huoli_buy_cli.query_all(huoli_consumers_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)


def update_huoli_buy_consumers_quarterly():
    start_date, end_date = DateUtil.get_last_quarter_date()
    huoli_consumers_sql = """
        select CONCAT(YEAR(po.createtime),',','Q',QUARTER(po.createtime)) s_day, count(DISTINCT po.userid),
        sum(po.totalprice) from product_order po
        left join pay on po.pay_id=pay.id
        where pay.paysource!='FREE' and po.account_type='hl'
        and po.status = 30
        and po.pid !=0
        and po.createtime >= %s
        and po.createtime < %s
    """

    insert_sql = """
        insert into huoli_buy_consumers_quarterly (s_day, consumers, consumers_amount, createtime, updatetime)
        values (%s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        consumers = values(consumers),
        consumers_amount = values(consumers_amount)
    """
    dto = [start_date, end_date]
    query_data = DBCli().huoli_buy_cli.query_all(huoli_consumers_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)


def update_huoli_buy_newconsumers_daily(days=0):
    """更新huolibuy的每日新增消费用户数, huoli_buy_newconsumers_daily"""
    start_date = DateUtil.get_date_before_days(int(days))
    end_date = DateUtil.get_date_after_days(1-int(days))
    huoli_consumers_sql = """
        select DATE_FORMAT(po.createtime, '%%Y-%%m-%%d') s_day, count(DISTINCT po.userid),
        sum(po.totalprice) from product_order po
        left join pay on po.pay_id=pay.id
        where pay.paysource!='FREE' and po.account_type='hl'
        and po.status = 30
        and po.pid !=0
        and po.createtime >= %s
        and po.createtime < %s
        and not EXISTS (select pp.userid from product_order pp
        left join pay on pp.pay_id=pay.id
        where pay.paysource!='FREE' and pp.account_type='hl'
        and po.status = 30
        and po.pid !=0
        and pp.createtime<%s and pp.userid=po.userid);
    """

    insert_sql = """
        insert into huoli_buy_newconsumers_daily (s_day, new_consumers, new_consumers_amount, createtime, updatetime)
        values (%s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        new_consumers = values(new_consumers),
        new_consumers_amount = values(new_consumers_amount)
    """
    dto = [start_date, end_date, start_date]
    query_data = DBCli().huoli_buy_cli.query_all(huoli_consumers_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)


def update_huoli_edj_newconsumers_daily(days=0):
    """更新huolibuy的e代价新增消费用户数, huoli_buy_edj_newconsumers_daily"""
    start_date = DateUtil.get_date_before_days(int(days))
    end_date = DateUtil.get_date_after_days(1-int(days))
    huoli_consumers_sql = """
        select DATE_FORMAT(po.createtime, '%%Y-%%m-%%d') s_day, count(DISTINCT po.userid),
        sum(po.totalprice) from product_order po
        left join pay on po.pay_id=pay.id
        where pay.paysource!='FREE' and po.account_type='edj'
        and po.status = 30
        and po.pid !=0
        and po.createtime >= %s
        and po.createtime < %s
        and not EXISTS (select pp.userid from product_order pp
        left join pay on pp.pay_id=pay.id
        where pay.paysource!='FREE' and pp.account_type='edj'
        and po.status = 30
        and po.pid !=0
        and pp.createtime<%s and pp.userid=po.userid);
    """

    insert_sql = """
        insert into huoli_buy_edj_newconsumers_daily (s_day, new_consumers, new_consumers_amount, createtime, updatetime)
        values (%s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        new_consumers = values(new_consumers),
        new_consumers_amount = values(new_consumers_amount)
    """
    dto = [start_date, end_date, start_date]
    query_data = DBCli().huoli_buy_cli.query_all(huoli_consumers_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)


if __name__ == '__main__':
    import datetime
    # start_date, end_date = DateUtil.get_last_week_date()
    # while start_date >= datetime.date(2017, 1, 16):
    #     print start_date
    #     update_huoli_buy_consumers_weekly(start_date)
    #     start_date, end_date = DateUtil.get_last_week_date(start_date)
    # update_huoli_buy_consumers_monthly()
    # update_huoli_buy_consumers_quarterly()

    # start_date, end_date = DateUtil.get_last_month_date()
    # while 1:
    #     update_huoli_buy_consumers_monthly(start_date)
    #     start_date, end_date = DateUtil.get_last_month_date(start_date)
    # update_huoli_buy_consumers_weekly()
    # update_huoli_buy_orders_daily(1)
    i = 1
    while i <= 280:
        update_huoli_edj_newconsumers_daily(i)
        i += 1

    # s = datetime.date(2017, 4, 1)
    # update_huoli_buy_consumers_quarterly(s)
    # update_huoli_buy_orders_daily(1)
    # update_huoli_buy_consumers_daily(1)