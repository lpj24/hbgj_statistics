# -*- coding: utf-8 -*-
from sql.gt_sqlHandlers import gtgj_consumers_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_gtgj_consumers_daily(days=0):
    if days > 0:
        today = DateUtil.date2str(DateUtil.getDateBeforeDays(3))
        tomorrow = DateUtil.date2str(DateUtil.getDateAfterDays(0))
    else:
        today = DateUtil.date2str(DateUtil.getDateBeforeDays(days))
        tomorrow = DateUtil.date2str(DateUtil.getDateAfterDays(1))
    dto = [today, tomorrow]
    query_data = DBCli().gt_cli.queryAll(gtgj_consumers_sql["gtgj_consumers_daily"], dto)
    DBCli().targetdb_cli.batchInsert(gtgj_consumers_sql["update_gtgj_consumers_daily"], query_data)


def update_gtgj_consumers_weekly():
    start_date = DateUtil.date2str(DateUtil.getLastWeekDate(DateUtil.getLastWeekDate()[0])[0])
    end_date = DateUtil.date2str(DateUtil.getLastWeekDate()[1])
    dto = [start_date, end_date, start_date, end_date]
    query_data = DBCli().gt_cli.queryAll(gtgj_consumers_sql["gtgj_consumers_weekly"], dto)
    DBCli().targetdb_cli.batchInsert(gtgj_consumers_sql["update_gtgj_consumers_weekly"], query_data)


def update_gtgj_consumers_monthly():
    start_date, end_date = DateUtil.getLastMonthDate()
    dto = [DateUtil.date2str(start_date, "%Y-%m-%d"), DateUtil.date2str(start_date), DateUtil.date2str(end_date),
           DateUtil.date2str(start_date), DateUtil.date2str(end_date)]
    query_data = DBCli().gt_cli.queryOne(gtgj_consumers_sql["gtgj_consumers_monthly"], dto)
    DBCli().targetdb_cli.insert(gtgj_consumers_sql["update_gtgj_consumers_monthly"], query_data)


def update_gtgj_consumers_quarterly():
    start_date, end_date = DateUtil.getLastQuarterDate()
    start_date = DateUtil.date2str(start_date)
    end_date = DateUtil.date2str(end_date)
    dto = [start_date, start_date, start_date, end_date, start_date, end_date]
    query_data = DBCli().gt_cli.queryOne(gtgj_consumers_sql["gtgj_consumers_quarterly"], dto)
    DBCli().targetdb_cli.insert(gtgj_consumers_sql["update_gtgj_consumers_quarterly"], query_data)

if __name__ == "__main__":
    #每小时更新当天数据  凌晨更新前三天
    update_gtgj_consumers_daily()
    update_gtgj_consumers_weekly()
    # update_gtgj_consumers_quarterly()