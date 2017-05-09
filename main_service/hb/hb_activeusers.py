# -*- coding: utf-8 -*-
from sql.hb_sqlHandlers import hb_activeusers_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_hbgj_activeusers_daily(days=0):
    """更新航班活跃用户(日), hbgj_activeusers_daily"""
    today = DateUtil.date2str(DateUtil.get_date_before_days(int(days)), '%Y-%m-%d')
    tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)), '%Y-%m-%d')
    dto = [today, today, tomorrow]
    query_data = DBCli().apibase_cli.queryOne(hb_activeusers_sql["hbgj_activeusers_daily"], dto)
    DBCli().targetdb_cli.insert(hb_activeusers_sql["update_hbgj_activeusers_daily"], query_data)
    return __file__


def update_hbgj_activeusers_weekly():
    start_date, end_date = DateUtil.get_last_week_date()
    start_date = DateUtil.date2str(start_date, '%Y-%m-%d')
    end_date = DateUtil.date2str(end_date, '%Y-%m-%d')
    dto = {"start_date": start_date, "end_date": end_date}
    query_data = DBCli().oracle_cli.queryOne(hb_activeusers_sql["hbgj_activeusers_weekly"], dto)
    DBCli().targetdb_cli.insert(hb_activeusers_sql["update_hbgj_activeusers_weekly"], query_data)


def update_hbgj_activeusers_monthly():
    start_date, end_date = DateUtil.get_last_month_date()
    start_date = DateUtil.date2str(start_date, '%Y-%m-%d')
    end_date = DateUtil.date2str(end_date, '%Y-%m-%d')
    dto = {"start_date": start_date, "end_date": end_date}
    query_data = DBCli().oracle_cli.queryOne(hb_activeusers_sql["hbgj_activeusers_monthly"], dto)
    DBCli().targetdb_cli.insert(hb_activeusers_sql["update_hbgj_activeusers_monthly"], query_data)

if __name__ == "__main__":
    # for x in xrange(6, 0, -1):
    # i = 5
    # while i >= 1:
    #     update_hbgj_activeusers_daily(i)
    #     i -= 1
    update_hbgj_activeusers_daily(1)