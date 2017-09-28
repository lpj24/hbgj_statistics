# -*- coding: utf-8 -*-
from sql.huoli_sqlHandlersHistory import eat_activeusers_daily_history_sql
from dbClient.db_client import targetdb_cli, huoli_cli
from dbClient.dateutil import DateUtil
import datetime


def insert_eat_activeusers_daily_history():
    today = DateUtil.get_date_after_days(0)
    table_start = datetime.date(2015, 12, 31)
    while today >= table_start:
        yes_date = DateUtil.add_days(today, -1)
        dto = [DateUtil.date2str(today, '%Y-%m-%d'), DateUtil.date_to_milli_seconds(yes_date),
               DateUtil.date_to_milli_seconds(today)]
        query_data = huoli_cli.query_one(eat_activeusers_daily_history_sql['eat_activeusers_daily_history'], dto)
        targetdb_cli.insert(eat_activeusers_daily_history_sql['insert_eat_activeusers_daily_history'], query_data)
        today = DateUtil.add_days(today, -1)


def insert_eat_activeusers_weekly_history():
    week_start, week_end = DateUtil.get_last_week_date()
    table_start = datetime.date(2015, 12, 28)
    while week_start >= table_start:
        dto = [DateUtil.date2str(week_start, '%Y-%m-%d'), DateUtil.date_to_milli_seconds(week_start),
               DateUtil.date_to_milli_seconds(week_end)]
        query_data = huoli_cli.query_one(eat_activeusers_daily_history_sql['eat_activeusers_weekly_history'], dto)
        targetdb_cli.insert(eat_activeusers_daily_history_sql['insert_eat_activeusers_weekly_history'], query_data)
        week_start, week_end = DateUtil.get_last_week_date(week_start)


def insert_eat_activeusers_monthly_history():
    month_start, month_end = DateUtil.get_last_month_date()
    table_start = datetime.date(2015, 12, 1)
    while month_start >= table_start:
        dto = [DateUtil.date2str(month_start, '%Y-%m-%d'), DateUtil.date_to_milli_seconds(month_start),
               DateUtil.date_to_milli_seconds(month_end)]
        query_data = huoli_cli.query_one(eat_activeusers_daily_history_sql['eat_activeusers_monthly_history'], dto)
        targetdb_cli.insert(eat_activeusers_daily_history_sql['insert_eat_activeusers_monthly_history'], query_data)
        month_start, month_end = DateUtil.get_last_month_date(month_start)


def insert_eat_activeusers_quarterly_history():
    quarter_start, quarter_end = DateUtil.get_last_quarter_date()
    min_date = datetime.date(2015, 12, 1)
    while quarter_end >= min_date:
        dto = [DateUtil.date2str(quarter_start, '%Y-%m-%d'), DateUtil.date2str(quarter_start, '%Y-%m-%d'),
               DateUtil.date_to_milli_seconds(quarter_start), DateUtil.date_to_milli_seconds(quarter_end)]
        query_data = huoli_cli.query_one(eat_activeusers_daily_history_sql['eat_activeusers_quarterly_history'], dto)

        targetdb_cli.insert(eat_activeusers_daily_history_sql['insert_eat_activeusers_quarterly_history'], query_data)
        quarter_start, quarter_end = DateUtil.get_last_quarter_date(quarter_start)

if __name__ == "__main__":
    insert_eat_activeusers_daily_history()
    insert_eat_activeusers_weekly_history()
    insert_eat_activeusers_monthly_history()
    insert_eat_activeusers_quarterly_history()