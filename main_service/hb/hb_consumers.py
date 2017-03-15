from sql.hb_sqlHandlers import hb_consumers_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_hb_consumers_daily(days=0):
    today_date = DateUtil.date2str(DateUtil.get_date_before_days(int(days)), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)), '%Y-%m-%d')
    dto = [today_date, end_date, today_date, end_date]
    query_data = DBCli().sourcedb_cli.queryOne(hb_consumers_sql['hb_consumers_daily'], dto)
    DBCli().targetdb_cli.insert(hb_consumers_sql['update_hb_consumers_daily'], query_data)


def update_hb_consumers_weekly(days=0):
    start_weekdate, end_weekdate = DateUtil.get_this_week_date()
    start_date = DateUtil.date2str(start_weekdate)
    end_date = DateUtil.date2str(end_weekdate)
    dto = [start_date, end_date, start_date, end_date]
    query_data = DBCli().sourcedb_cli.queryOne(hb_consumers_sql['hb_consumers_weekly'], dto)
    DBCli().targetdb_cli.insert(hb_consumers_sql['update_hb_consumers_weekly'], query_data)


def update_hb_consumers_monthly(days=0):
    start_monthdate, end_enddate = DateUtil.get_this_month_date()
    start_monthdate = DateUtil.date2str(start_monthdate)
    end_enddate = DateUtil.date2str(end_enddate)
    dto = [start_monthdate, end_enddate, start_monthdate, end_enddate]
    query_data = DBCli().sourcedb_cli.queryOne(hb_consumers_sql['hb_consumers_monthly'], dto)
    DBCli().targetdb_cli.insert(hb_consumers_sql['update_hb_consumers_monthly'], query_data)


def update_hb_consumers_quarterly():
    start_quarterdate, end_quarterdate = DateUtil.get_last_quarter_date()
    start_quarterdate = DateUtil.date2str(start_quarterdate)
    end_quarterdate = DateUtil.date2str(end_quarterdate)
    dto = [start_quarterdate, end_quarterdate, start_quarterdate, end_quarterdate]
    query_data = DBCli().sourcedb_cli.queryOne(hb_consumers_sql['hb_consumers_quarterly'], dto)
    DBCli().targetdb_cli.insert(hb_consumers_sql['update_hb_consumers_daily'], query_data)


def update_hb_newconsumers_daily(days=0):
    yesterday = DateUtil.get_before_days(days)
    dto = [yesterday, yesterday, yesterday, yesterday]
    query_data = DBCli().sourcedb_cli.queryOne(hb_consumers_sql['hb_newconsumers_daily'], dto)
    DBCli().targetdb_cli.insert(hb_consumers_sql['update_hb_newconsumers_daily'], query_data)

if __name__ == "__main__":
    # update_hb_consumers_daily(1)
    # update_hb_newconsumers_daily(1)
    # update_hb_consumers_weekly()
    update_hb_consumers_monthly()
    # update_hb_consumers_quarterly()