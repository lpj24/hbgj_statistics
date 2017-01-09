# -*- coding: utf-8 -*-
from sql.huoli_sqlHandlersHistory import car_history
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import datetime


def insert_car_orders_daily_history():
    today = DateUtil.get_date_after_days(0)
    dto = []
    # for i in xrange(4):
    #     dto.append(DateUtil.date2str(today))
    # query_data = car_cli.queryAll(car_history["car_orders_daily_history"], dto)
    # targetdb_cli.batchInsert(car_history["insert_car_orders_daily_history"], query_data)

    #只分接送机和接送站
    for i in xrange(2):
        dto.append(DateUtil.date2str(today))
    print dto
    query_data = DBCli().car_cli.queryAll(car_history["car_orders_jz_daily_history"], dto)
    DBCli().targetdb_cli.batchInsert(car_history["insert_car_orders_jz_daily_history"], query_data)


def insert_car_consumers_daily_history():
    today = DateUtil.get_date_after_days(0)
    dto = []
    # for i in xrange(4):
    #     dto.append(DateUtil.date2str(today))
    # query_data = car_cli.queryAll(car_history["car_consumers_daily_history"], dto)
    # targetdb_cli.batchInsert(car_history["insert_car_consumers_daily_history"], query_data)

    #接送机与接送站
    for i in xrange(3):
        dto.append(DateUtil.date2str(today))
    query_data = DBCli().car_cli.queryAll(car_history["car_consumers_jz_daily_history"], dto)
    DBCli().targetdb_cli.batchInsert(car_history["insert_car_consumers_jz_daily_history"], query_data)


def insert_car_newconsumers_daily_history():
    # today = DateUtil.get_date_after_days(0)
    today = datetime.date(2014, 6, 2)
    min_date = datetime.date(2014, 1, 1)
    while today >= min_date:
        dto = []
        for i in xrange(3):
            dto.append(DateUtil.date2str(today, '%Y-%m-%d'))
            dto.append(DateUtil.date2str(today, '%Y-%m-%d'))
            dto.append(DateUtil.date2str(today, '%Y-%m-%d'))
        query_data = DBCli().car_cli.queryOne(car_history["car_newconsumers_daily_history"], dto)
        print query_data
        DBCli().targetdb_cli.insert(car_history["insert_car_newconsumers_daily_history"], query_data)
        today = DateUtil.add_days(today, -1)


def insert_car_consumers_weekly_history():
    start_week, end_week = DateUtil.get_last_week_date()
    dto = []
    # for i in xrange(4):
    #     dto.append(DateUtil.date2str(DateUtil.add_days(end_week, -1)))
    # query_data = car_cli.queryAll(car_history["car_consumers_weekly_history"], dto)
    # targetdb_cli.batchInsert(car_history["insert_car_consumers_weekly_history"], query_data)

    for i in xrange(3):
        dto.append(DateUtil.date2str(DateUtil.add_days(end_week, -1)))
    query_data = DBCli().car_cli.queryAll(car_history["car_consumers_jz_weekly_history"], dto)
    DBCli().targetdb_cli.batchInsert(car_history["insert_car_consumers_jz_weekly_history"], query_data)


def insert_car_consumers_monthly_history():
    start_month, end_month = DateUtil.get_last_month_date()
    dto = []
    # for i in xrange(4):
    #     dto.append(DateUtil.date2str(end_month))
    # query_data = car_cli.queryAll(car_history["car_consumers_monthly_history"], dto)
    # targetdb_cli.batchInsert(car_history["insert_car_consumers_monthly_history"], query_data)

    for i in xrange(3):
        dto.append(DateUtil.date2str(end_month))
    query_data = DBCli().car_cli.queryAll(car_history["car_consumers_jz_monthly_history"], dto)
    DBCli().targetdb_cli.batchInsert(car_history["insert_car_consumers_jz_monthly_history"], query_data)


def insert_car_consumers_quarterly_history():
    # query_data = car_cli.queryAll(car_history["car_consumers_quarterly_history"])
    # targetdb_cli.batchInsert(car_history["insert_car_consumers_quarterly_history"], query_data)

    query_data = DBCli().car_cli.queryAll(car_history["car_consumers_jz_quarterly_history"])
    DBCli().targetdb_cli.batchInsert(car_history["insert_car_consumers_jz_quarterly_history"], query_data)


if __name__ == "__main__":
    # insert_car_orders_daily_history()
    # insert_car_consumers_daily_history()
    insert_car_newconsumers_daily_history()
    # insert_car_consumers_weekly_history()
    # insert_car_consumers_monthly_history()
    # insert_car_consumers_quarterly_history()