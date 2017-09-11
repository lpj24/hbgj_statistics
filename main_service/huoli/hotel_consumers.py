# -*- coding: utf-8 -*-
from dbClient.dateutil import DateUtil
from dbClient.db_client import DBCli
from sql.huoli_sqlHandlers import hotel_consumers_sql


def update_hotel_consumers_daily(days=0):
    """更新酒店消费(日), hotel_consumers_daily"""
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - int(days))
    dto = [DateUtil.date2str(start_date), DateUtil.date2str(end_date),
           DateUtil.date2str(start_date), DateUtil.date2str(end_date)]
    query_data = DBCli().tongji_skyhotel_cli.queryOne(hotel_consumers_sql["hotel_consumers_daily"], dto)
    DBCli().targetdb_cli.insert(hotel_consumers_sql["update_hotel_consumers_daily"], query_data)
    return __file__


def update_hotel_consumers_weekly():
    """更新酒店消费用户, hotel_consumers_weekly"""
    start_date, end_date = DateUtil.get_last_week_date()
    dto = [DateUtil.date2str(start_date), DateUtil.date2str(end_date),
           DateUtil.date2str(start_date), DateUtil.date2str(end_date)]
    query_data = DBCli().tongji_skyhotel_cli.queryOne(hotel_consumers_sql["hotel_consumers_weekly"], dto)
    DBCli().targetdb_cli.insert(hotel_consumers_sql["update_hotel_consumers_weekly"], query_data)


def update_hotel_consumers_monthly():
    start_date, end_date = DateUtil.get_last_month_date()
    dto = [DateUtil.date2str(start_date), DateUtil.date2str(end_date),
           DateUtil.date2str(start_date), DateUtil.date2str(end_date)]
    query_data = DBCli().tongji_skyhotel_cli.queryOne(hotel_consumers_sql["hotel_consumers_monthly"], dto)
    DBCli().targetdb_cli.insert(hotel_consumers_sql["update_hotel_consumers_monthly"], query_data)

if __name__ == "__main__":
    update_hotel_consumers_daily(2)
    # update_hotel_consumers_weekly()
    # update_hotel_consumers_monthly()