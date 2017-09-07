# -*- coding: utf-8 -*-
from dbClient.dateutil import DateUtil
from dbClient.db_client import DBCli


def update_huoli_buy_orders_daily(days=0):
    """更新huolibuy的每日订单数, huoli_buy_orders_daily"""
    start_date = DateUtil.get_date_before_days(int(days) * 3)
    end_date = DateUtil.get_date_after_days(1-int(days))
    huoli_order_sql = """
        select DATE_FORMAT(po.createtime, '%%Y-%%m-%%d') s_day, count(DISTINCT po.id) from product_order po
        left join pay on po.pay_id=pay.id
        where pay.paysource!='FREE' and po.account_type='hl'
        and po.createtime >= %s
        and po.createtime < %s
        GROUP BY s_day
    """

    insert_sql = """
        insert into huoli_buy_orders_daily (s_day, orders_num, createtime, updatetime)
        values (%s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        orders_num = values(orders_num)
    """
    dto = [start_date, end_date]
    query_data = DBCli().huoli_buy_cli.queryAll(huoli_order_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_sql, query_data)


def update_huoli_buy_consumers_daily(days=0):
    """更新huolibuy的每日订单数, huoli_buy_consumers_daily"""
    start_date = DateUtil.get_date_before_days(int(days) * 3)
    end_date = DateUtil.get_date_after_days(1-int(days))
    huoli_consumers_sql = """
        select DATE_FORMAT(po.createtime, '%%Y-%%m-%%d') s_day, count(DISTINCT po.userid) from product_order po
        left join pay on po.pay_id=pay.id
        where pay.paysource!='FREE' and po.account_type='hl'
        and po.createtime >= %s
        and po.createtime < %s
        GROUP BY s_day
    """

    insert_sql = """
        insert into huoli_buy_consumers_daily (s_day, consumers_num, createtime, updatetime)
        values (%s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        consumers_num = values(consumers_num)
    """
    dto = [start_date, end_date]
    query_data = DBCli().huoli_buy_cli.queryAll(huoli_consumers_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_sql, query_data)


if __name__ == '__main__':
    update_huoli_buy_orders_daily(1)
