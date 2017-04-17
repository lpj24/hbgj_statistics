# -*- coding: utf-8 -*-
from sql.huoli_sqlHandlers import car_consumers_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_car_consumers_daily(days=0):
    """更新专车消费(日), huoli_car_consumers_daily"""

    today = DateUtil.get_date_before_days(int(days))
    tomorrow = DateUtil.get_date_after_days(1-int(days))
    dto = [DateUtil.date2str(today, '%Y-%m-%d'), DateUtil.date2str(today), DateUtil.date2str(tomorrow)] * 3
    query_data = DBCli().car_cli.queryOne(car_consumers_sql["car_consumers_jz_daily"], dto)
    DBCli().targetdb_cli.insert(car_consumers_sql["update_car_consumers_jz_daily"], query_data)
    return __file__


def update_car_consumers_weekly():
    start_date, end_date = DateUtil.get_last_week_date()
    dto = []
    # for i in xrange(4):
    #     dto.append(DateUtil.date2str(start_date, '%Y-%m-%d'))
    #     dto.append(DateUtil.date2str(start_date))
    #     dto.append(DateUtil.date2str(end_date))
    #
    # query_data = car_cli.queryOne(car_consumers_sql['car_consumers_weekly'], dto)
    # targetdb_cli.insert(car_consumers_sql['update_car_consumers_weekly'], query_data)

    for i in xrange(3):
        dto.append(DateUtil.date2str(start_date, '%Y-%m-%d'))
        dto.append(DateUtil.date2str(start_date))
        dto.append(DateUtil.date2str(end_date))

    query_data = DBCli().car_cli.queryOne(car_consumers_sql["car_consumers_jz_weekly"], dto)
    DBCli().targetdb_cli.insert(car_consumers_sql["update_car_consumers_jz_weekly"], query_data)


def update_car_consumers_monthly():
    start_date, end_date = DateUtil.get_last_month_date()
    dto = []
    # for i in xrange(4):
    #     dto.append(DateUtil.date2str(start_date, '%Y-%m-%d'))
    #     dto.append(DateUtil.date2str(start_date))
    #     dto.append(DateUtil.date2str(end_date))
    # query_data = car_cli.queryOne(car_consumers_sql['car_consumers_monthly'], dto)
    # targetdb_cli.insert(car_consumers_sql['update_car_consumers_monthly'], query_data)

    for i in xrange(3):
        dto.append(DateUtil.date2str(start_date, '%Y-%m-%d'))
        dto.append(DateUtil.date2str(start_date))
        dto.append(DateUtil.date2str(end_date))
    query_data = DBCli().car_cli.queryOne(car_consumers_sql["car_consumers_jz_monthly"], dto)
    DBCli().targetdb_cli.insert(car_consumers_sql["update_car_consumers_jz_monthly"], query_data)


def update_car_consumers_quarterly():
    start_date, end_date = DateUtil.get_last_quarter_date()
    dto = []
    # for i in xrange(4):
    #     dto.append(DateUtil.date2str(start_date, '%Y-%m-%d'))
    #     dto.append(DateUtil.date2str(start_date, '%Y-%m-%d'))
    #     dto.append(DateUtil.date2str(start_date))
    #     dto.append(DateUtil.date2str(end_date))
    # query_data = car_cli.queryOne(car_consumers_sql['car_consumers_quarterly'], dto)
    # targetdb_cli.insert(car_consumers_sql['update_car_consumers_quarterly'], query_data)

    for i in xrange(3):
        dto.append(DateUtil.date2str(start_date, '%Y-%m-%d'))
        dto.append(DateUtil.date2str(start_date, '%Y-%m-%d'))
        dto.append(DateUtil.date2str(start_date))
        dto.append(DateUtil.date2str(end_date))
    query_data = DBCli().car_cli.queryOne(car_consumers_sql["car_consumers_jz_quarterly"], dto)
    DBCli().targetdb_cli.insert(car_consumers_sql["update_car_consumers_jz_quarterly"], query_data)


def update_car_newconsumers_daily(days=0):
    """更新专车新增消费用户(日), huoli_car_newconsumers_daily"""
    today = DateUtil.get_date_before_days(int(days))
    dto = []
    for i in xrange(3):
        dto.append(DateUtil.date2str(today, '%Y-%m-%d'))
        dto.append(DateUtil.date2str(today, '%Y-%m-%d'))
        dto.append(DateUtil.date2str(today, '%Y-%m-%d'))
    query_data = DBCli().car_cli.queryOne(car_consumers_sql['car_newconsumers_daily'], dto)
    DBCli().targetdb_cli.insert(car_consumers_sql['update_car_newconsumers_daily'], query_data)
    return __file__

if __name__ == "__main__":
    update_car_consumers_daily(2)
    # update_car_consumers_weekly()
    # for i in xrange(4, 0, -1):
    #     update_car_newconsumers_daily(i)
    # update_car_consumers_monthly()
    # update_car_consumers_quarterly()
    # update_car_newconsumers_daily()