# -*- coding: utf-8 -*-
from sql.huoli_sqlHandlersHistory import eat_activeusers_daily_history_sql
from dbClient.db_client import targetdb_cli, huoli_cli
from dbClient.dateutil import DateUtil
import datetime


def insert_eat_activeusers_daily_history():
    today = DateUtil.getDateAfterDays(0)
    table_start = datetime.date(2015, 12, 31)
    while today >= table_start:
        yes_date = DateUtil.add_days(today, -1)
        dto = [DateUtil.date2str(today, '%Y-%m-%d'), DateUtil.dateToMilliseconds(yes_date),
               DateUtil.dateToMilliseconds(today)]
        query_data = huoli_cli.queryOne(eat_activeusers_daily_history_sql['eat_activeusers_daily_history'], dto)
        targetdb_cli.insert(eat_activeusers_daily_history_sql['insert_eat_activeusers_daily_history'], query_data)
        today = DateUtil.add_days(today, -1)


def insert_eat_activeusers_weekly_history():
    week_start, week_end = DateUtil.getLastWeekDate()
    table_start = datetime.date(2015, 12, 28)
    while week_start >= table_start:
        dto = [DateUtil.date2str(week_start, '%Y-%m-%d'), DateUtil.dateToMilliseconds(week_start),
               DateUtil.dateToMilliseconds(week_end)]
        query_data = huoli_cli.queryOne(eat_activeusers_daily_history_sql['eat_activeusers_weekly_history'], dto)
        targetdb_cli.insert(eat_activeusers_daily_history_sql['insert_eat_activeusers_weekly_history'], query_data)
        week_start, week_end = DateUtil.getLastWeekDate(week_start)


def insert_eat_activeusers_monthly_history():
    month_start, month_end = DateUtil.getLastMonthDate()
    table_start = datetime.date(2015, 12, 1)
    while month_start >= table_start:
        dto = [DateUtil.date2str(month_start, '%Y-%m-%d'), DateUtil.dateToMilliseconds(month_start),
               DateUtil.dateToMilliseconds(month_end)]
        query_data = huoli_cli.queryOne(eat_activeusers_daily_history_sql['eat_activeusers_monthly_history'], dto)
        targetdb_cli.insert(eat_activeusers_daily_history_sql['insert_eat_activeusers_monthly_history'], query_data)
        month_start, month_end = DateUtil.getLastMonthDate(month_start)


def insert_eat_activeusers_quarterly_history():
    quarter_start, quarter_end = DateUtil.getLastQuarterDate()
    min_date = datetime.date(2015, 12, 1)
    while quarter_end >= min_date:
        dto = [DateUtil.date2str(quarter_start, '%Y-%m-%d'), DateUtil.date2str(quarter_start, '%Y-%m-%d'),
               DateUtil.dateToMilliseconds(quarter_start), DateUtil.dateToMilliseconds(quarter_end)]
        query_data = huoli_cli.queryOne(eat_activeusers_daily_history_sql['eat_activeusers_quarterly_history'], dto)

        targetdb_cli.insert(eat_activeusers_daily_history_sql['insert_eat_activeusers_quarterly_history'], query_data)
        quarter_start, quarter_end = DateUtil.getLastQuarterDate(quarter_start)

if __name__ == "__main__":
    insert_eat_activeusers_daily_history()
    insert_eat_activeusers_weekly_history()
    insert_eat_activeusers_monthly_history()
    insert_eat_activeusers_quarterly_history()