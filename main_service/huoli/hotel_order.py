# -*- coding: utf-8 -*-
from dbClient.dateutil import DateUtil
from dbClient.db_client import DBCli
from sql.huoli_sqlHandlers import hotel_orders_sql


def update_hotel_orders_daily(days=0):
    """酒店订单(日), hotel_orders_daily"""
    if days > 0:
        start_date = DateUtil.date2str(DateUtil.get_date_before_days(7), '%Y-%m-%d')
        end_date = DateUtil.date2str(DateUtil.get_date_after_days(0), '%Y-%m-%d')
    else:
        end_date = DateUtil.date2str(DateUtil.get_date_after_days(1), '%Y-%m-%d')
        start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')

    dto = [start_date, end_date, start_date, end_date]
    query_data = DBCli().tongji_skyhotel_cli.query_all(hotel_orders_sql["hotel_orders_daily"], dto)
    DBCli().targetdb_cli.batch_insert(hotel_orders_sql["update_hotel_orders_daily"], query_data)
    pass

if __name__ == "__main__":
    update_hotel_orders_daily(1)
