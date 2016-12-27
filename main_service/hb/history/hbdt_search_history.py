#coding:utf8
from sql.hb_sqlHandlersHistory import hb_flight_search_user_history_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import datetime


def insert_hbdt_search_history():
    today = DateUtil.getDateAfterDays(0)
    table_start = datetime.date(2015, 12, 1)
    total_days = (today - table_start).days
    for i in xrange(total_days):
        yes_date = DateUtil.add_days(today, -1)
        tablename = DateUtil.getTable(yes_date)
        dto = [DateUtil.date2str(yes_date, '%Y-%m-%d'), DateUtil.date2str(yes_date), DateUtil.date2str(today), tablename]
        query_data = DBCli().Apilog_cli.queryOne(hb_flight_search_user_history_sql['hb_filght_search_user_daily_history'], dto)
        print query_data
        DBCli().targetdb_cli.insert(hb_flight_search_user_history_sql["update_flight_search_user_pv_daily_history"], query_data)
        today = DateUtil.add_days(today, -1)


def insert_hbdt_search_weekly_history():
    last_mon, this_mon = DateUtil.getLastWeekDate()
    table_start_date = datetime.date(2015, 12, 1)
    while last_mon >= table_start_date:
        start_table = DateUtil.getTable(DateUtil.add_days(last_mon, -1))
        end_table = DateUtil.getTable(this_mon)
        if start_table != end_table:
            dto = [DateUtil.date2str(last_mon, '%Y-%m-%d'), DateUtil.date2str(last_mon), DateUtil.date2str(this_mon),
                   DateUtil.date2str(last_mon), DateUtil.date2str(this_mon), start_table, end_table]
            query_data = DBCli().Apilog_cli.queryOne(
                hb_flight_search_user_history_sql['hb_filght_search_user_difftable_weekly'], dto)
            DBCli().targetdb_cli.insert(hb_flight_search_user_history_sql["update_flight_search_user_pv_weekly_history"],
                                query_data)
        else:
            dto = [DateUtil.date2str(last_mon, '%Y-%m-%d'), DateUtil.date2str(last_mon),
                   DateUtil.date2str(this_mon), start_table]
            query_data = DBCli().Apilog_cli.queryOne(
                hb_flight_search_user_history_sql['hb_filght_search_user_weeky_history'], dto)
            DBCli().targetdb_cli.insert(hb_flight_search_user_history_sql["update_flight_search_user_pv_weekly_history"],
                                query_data)
        last_mon, this_mon = DateUtil.getLastWeekDate(last_mon)


def insert_hbdt_search_monthly_history():
    start_date, end_date = DateUtil.getLastMonthDate()
    table_start_date = datetime.date(2015, 12, 1)
    while start_date >= table_start_date:
        table_list = DateUtil.getAllTable(start_date.year, start_date.month)
        dto = [DateUtil.date2str(start_date, '%Y-%m-%d'), table_list[0], table_list[1], table_list[2]]
        query_data = DBCli().Apilog_cli.queryOne(hb_flight_search_user_history_sql['hb_filght_search_user_monthly_history'], dto)
        DBCli().targetdb_cli.insert(hb_flight_search_user_history_sql["update_flight_search_user_pv_monthly_history"], query_data)
        start_date, end_date = DateUtil.getLastMonthDate(start_date)


def insert_hbdt_search_quarterly_history():
    start_date, end_date = DateUtil.getLastQuarterDate()
    table_start_date = datetime.date(2015, 10, 1)
    while start_date >= table_start_date:
        dto = [DateUtil.date2str(start_date, '%Y-%m-%d'), DateUtil.date2str(start_date, '%Y-%m-%d')]
        start_index = start_date.month
        if end_date.month < start_index:
            end_index=13
        else:
            end_index = end_date.month
        for tablelist in xrange(start_index, end_index):
            table_list = DateUtil.getAllTable(start_date.year, tablelist)
            dto.extend(table_list)
        query_data = DBCli().Apilog_cli.queryOne(hb_flight_search_user_history_sql['hb_filght_search_user_quarterly_history'], dto)
        DBCli().targetdb_cli.insert(hb_flight_search_user_history_sql["update_flight_search_user_pv_quarterly_history"], query_data)
        start_date, end_date = DateUtil.getLastQuarterDate(start_date)

if __name__ == "__main__":
    insert_hbdt_search_history()