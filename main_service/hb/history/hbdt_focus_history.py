# -*- coding: utf-8 -*-
from sql.hb_sqlHandlersHistory import hb_flight_focus_user_history_sql
from dbClient.db_client import oracle_cli, targetdb_cli
from dbClient.dateutil import DateUtil
import datetime


def insert_hbdt_focus_daily_history():
    today = DateUtil.getDateBeforeDays(0)
    min_date = datetime.date(2015, 4, 13)
    while today >= min_date:
        yes_date = DateUtil.add_days(today, -1)
        dto = {'s_day': DateUtil.date2str(yes_date, '%Y-%m-%d'), 'start_date': DateUtil.date2str(yes_date, '%Y%m%d'),
               'end_date': DateUtil.date2str(today, '%Y%m%d')}
        today = DateUtil.add_days(today, -1)
        query_data = oracle_cli.queryOne(hb_flight_focus_user_history_sql['hb_flight_focus_users_daily_history'], dto)
        targetdb_cli.insert(hb_flight_focus_user_history_sql['update_flight_focus_user_daily_history'], query_data)


def insert_hbdt_focus_weekly_history():
    week_start, week_end = DateUtil.getLastWeekDate()
    min_date = datetime.date(2015, 4, 13)
    while week_start >= min_date:
        dto = {'s_day': DateUtil.date2str(week_start, '%Y-%m-%d'), 'end_date': DateUtil.date2str(week_end),
               'start_date': DateUtil.date2str(week_start)}
        query_data = oracle_cli.queryOne(hb_flight_focus_user_history_sql['hb_flight_focus_users_weekly_history'], dto)
        targetdb_cli.insert(hb_flight_focus_user_history_sql['update_flight_focus_user_weekly_history'], query_data)
        week_start, week_end = DateUtil.getLastWeekDate(week_start)


def insert_hbdt_focus_monthly_history():
    month_start, month_end = DateUtil.getLastMonthDate()
    min_date = datetime.date(2015, 4, 13)
    while month_start >= min_date:
        dto = {'s_day': DateUtil.date2str(month_start, '%Y-%m-%d'), 'end_date': DateUtil.date2str(month_end),
               'start_date': DateUtil.date2str(month_start)}
        query_data = oracle_cli.queryOne(hb_flight_focus_user_history_sql['hb_flight_focus_users_monthly_history'], dto)

        targetdb_cli.insert(hb_flight_focus_user_history_sql['update_flight_focus_user_monthly_history'], query_data)
        month_start, month_end = DateUtil.getLastMonthDate(month_start)


def insert_hbdt_focus_quarterly_history():
    quarter_start, quarter_end = DateUtil.getLastQuarterDate()
    min_date = datetime.date(2015, 4, 13)
    while quarter_start >= min_date:
        dto = {'s_day': DateUtil.date2str(quarter_start, '%Y-%m-%d'), 'end_date': DateUtil.date2str(quarter_end),
               'start_date': DateUtil.date2str(quarter_start)}
        query_data = oracle_cli.queryOne(hb_flight_focus_user_history_sql['hb_flight_focus_users_quarterly_history'], dto)

        targetdb_cli.insert(hb_flight_focus_user_history_sql['update_flight_focus_user_quarterly_history'], query_data)
        quarter_start, quarter_end = DateUtil.getLastQuarterDate(quarter_start)


if __name__ == "__main__":
    insert_hbdt_focus_daily_history()
    insert_hbdt_focus_weekly_history()
    insert_hbdt_focus_monthly_history()
    insert_hbdt_focus_quarterly_history()