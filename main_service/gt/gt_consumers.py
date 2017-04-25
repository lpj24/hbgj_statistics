# -*- coding: utf-8 -*-
from sql.gt_sqlHandlers import gtgj_consumers_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_gtgj_consumers_daily(days=0):
    """更新高铁消费用户, gtgj_consumers_daily"""
    if days > 0:
        today = DateUtil.date2str(DateUtil.get_date_before_days(3))
        tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(0))
    else:
        today = DateUtil.date2str(DateUtil.get_date_before_days(days))
        tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(1))
    dto = [today, tomorrow]
    query_data = DBCli().gt_cli.queryAll(gtgj_consumers_sql["gtgj_consumers_daily"], dto)
    DBCli().targetdb_cli.batchInsert(gtgj_consumers_sql["update_gtgj_consumers_daily"], query_data)
    return __file__


def update_gtgj_consumers_weekly():
    start_date = DateUtil.date2str(DateUtil.get_last_week_date(DateUtil.get_last_week_date()[0])[0])
    end_date = DateUtil.date2str(DateUtil.get_last_week_date()[1])
    dto = [start_date, end_date, start_date, end_date]
    query_data = DBCli().gt_cli.queryAll(gtgj_consumers_sql["gtgj_consumers_weekly"], dto)
    DBCli().targetdb_cli.batchInsert(gtgj_consumers_sql["update_gtgj_consumers_weekly"], query_data)


def update_gtgj_consumers_monthly():
    start_date, end_date = DateUtil.get_last_month_date()
    dto = [DateUtil.date2str(start_date, "%Y-%m-%d"), DateUtil.date2str(start_date), DateUtil.date2str(end_date),
           DateUtil.date2str(start_date), DateUtil.date2str(end_date)]
    query_data = DBCli().gt_cli.queryOne(gtgj_consumers_sql["gtgj_consumers_monthly"], dto)
    DBCli().targetdb_cli.insert(gtgj_consumers_sql["update_gtgj_consumers_monthly"], query_data)


def update_gtgj_consumers_quarterly():
    start_date, end_date = DateUtil.get_last_quarter_date()
    start_date = DateUtil.date2str(start_date)
    end_date = DateUtil.date2str(end_date)
    dto = [start_date, start_date, start_date, end_date, start_date, end_date]
    query_data = DBCli().gt_cli.queryOne(gtgj_consumers_sql["gtgj_consumers_quarterly"], dto)
    DBCli().targetdb_cli.insert(gtgj_consumers_sql["update_gtgj_consumers_quarterly"], query_data)

if __name__ == "__main__":
    #每小时更新当天数据  凌晨更新前三天
    update_gtgj_consumers_daily(1)
    # update_gtgj_consumers_weekly()
    # update_gtgj_consumers_quarterly()