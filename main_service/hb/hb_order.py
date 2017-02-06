# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from sql.hb_sqlHandlers import hb_orders_date_sql


def update_hb_gt_order_daily(days=0):
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(int(days)), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)), '%Y-%m-%d')
    dto = [start_date, end_date]
    query_data = DBCli().sourcedb_cli.queryOne(hb_orders_date_sql["hb_gt_order_daily_sql"], dto)
    DBCli().targetdb_cli.insert(hb_orders_date_sql["update_hb_gt_order_daily_sql"], query_data)


def update_hb_gt_order_daily_his():
    import datetime
    start_date = datetime.date(2012, 11, 22)
    end_date = datetime.date(2017, 2, 6)
    dto = [start_date, end_date]
    query_data = DBCli().sourcedb_cli.queryAll(hb_orders_date_sql["hb_gt_order_daily_sql"], dto)
    DBCli().targetdb_cli.batchInsert(hb_orders_date_sql["update_hb_gt_order_daily_sql"], query_data)

if __name__ == "__main__":
    update_hb_gt_order_daily(1)
    update_hb_gt_order_daily_his()