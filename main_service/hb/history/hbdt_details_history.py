# -*- coding: utf-8 -*-
from sql.hb_sqlHandlersHistory import hb_flight_detail_user_history_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import datetime


def insert_hbdt_details_history():
    today = DateUtil.get_date_after_days(0)
    table_start = datetime.date(2015, 12, 1)
    total_days = (today - table_start).days
    for i in xrange(total_days):
        yes_date = DateUtil.add_days(today, -1)
        tablename = DateUtil.get_table(yes_date)
        dto = [DateUtil.date2str(yes_date), DateUtil.date2str(today), tablename]
        # query_data = Apilog_cli.queryOne(hb_flight_detail_user_history_sql['hb_filght_detail_user_daily_history'], dto)
        # targetdb_cli.insert(hb_flight_detail_user_history_sql['update_flight_detail_user_daily_history'], query_data)
        query_data = DBCli().Apilog_cli.queryOne(hb_flight_detail_user_history_sql["flight_detail_user_query_daily_his"], dto)
        print query_data
        DBCli().targetdb_cli.insert(hb_flight_detail_user_history_sql["update_flight_detail_user_query_daily_his"], query_data)
        today = DateUtil.add_days(today, -1)


def insert_hbdt_details_weekly_history():
    last_mon, this_mon = DateUtil.get_last_week_date()
    table_start_date = datetime.date(2015, 12, 1)
    while last_mon >= table_start_date:
        start_table = DateUtil.get_table(DateUtil.add_days(last_mon, -1))
        end_table = DateUtil.get_table(this_mon)
        if start_table != end_table:
            dto = [DateUtil.date2str(last_mon, '%Y-%m-%d'), DateUtil.date2str(last_mon), DateUtil.date2str(this_mon),
                   DateUtil.date2str(last_mon), DateUtil.date2str(this_mon), start_table, end_table]
            query_data = DBCli().Apilog_cli.queryOne(
                hb_flight_detail_user_history_sql['hb_filght_detail_user_difftable_weekly'], dto)
            print query_data
            DBCli().targetdb_cli.insert(hb_flight_detail_user_history_sql['update_flight_detail_user_query_weekly_his'],
                                query_data)
        else:
            dto = [DateUtil.date2str(last_mon, '%Y-%m-%d'), DateUtil.date2str(last_mon),
                   DateUtil.date2str(this_mon), start_table]
            query_data = DBCli().Apilog_cli.queryOne(
                hb_flight_detail_user_history_sql['hb_filght_detail_user_weeky_history'], dto)
            print query_data
            DBCli().targetdb_cli.insert(hb_flight_detail_user_history_sql['update_flight_detail_user_query_weekly_his'],
                                query_data)
        last_mon, this_mon = DateUtil.get_last_week_date(last_mon)


def insert_hbdt_details_monthly_history():
    start_date, end_date = DateUtil.get_last_month_date()
    table_start_date = datetime.date(2015, 12, 1)
    while start_date >= table_start_date:
        table_list = DateUtil.get_all_table(start_date.year, start_date.month)
        dto = [DateUtil.date2str(start_date, '%Y-%m-%d'), table_list[0], table_list[1], table_list[2]]
        query_data = DBCli().Apilog_cli.queryOne(hb_flight_detail_user_history_sql['hb_filght_detail_user_monthly_history'], dto)
        print query_data
        DBCli().targetdb_cli.insert(hb_flight_detail_user_history_sql["update_flight_detail_user_query_monthly_his"], query_data)
        start_date, end_date = DateUtil.get_last_month_date(start_date)


def insert_hbdt_details_quarterly_history():
    start_date, end_date = DateUtil.get_last_quarter_date()
    table_start_date = datetime.date(2015, 10, 1)
    while start_date >= table_start_date:
        dto = [DateUtil.date2str(start_date, '%Y-%m-%d'), DateUtil.date2str(start_date, '%Y-%m-%d')]
        start_index = start_date.month
        if end_date.month < start_index:
            end_index=13
        else:
            end_index = end_date.month
        for tablelist in xrange(start_index, end_index):
            table_list = DateUtil.get_all_table(start_date.year, tablelist)
            dto.extend(table_list)
        query_data = DBCli().Apilog_cli.queryOne(hb_flight_detail_user_history_sql['hb_filght_detail_user_quarterly_history'], dto)
        DBCli().targetdb_cli.insert(hb_flight_detail_user_history_sql["update_flight_detail_user_query_quarterly_his"], query_data)
        start_date, end_date = DateUtil.get_last_quarter_date(start_date)

if __name__ == "__main__":
    # insert_hbdt_details_history()
    # insert_hbdt_details_weekly_history()
    insert_hbdt_details_monthly_history()