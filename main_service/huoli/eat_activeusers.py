from sql.huoli_sqlHandlers import eat_activeusers_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_eat_active_user_daily(days=0):
    today = DateUtil.get_date_before_days(int(days))
    tomorrow = DateUtil.get_date_after_days(1-int(days))
    dto = [DateUtil.date2str(today, '%Y-%m-%d'), DateUtil.date_to_milli_seconds(today),
           DateUtil.date_to_milli_seconds(tomorrow)]
    query_data = DBCli().huoli_cli.queryOne(eat_activeusers_sql['eat_activeusers_daily'], dto)
    DBCli().targetdb_cli.insert(eat_activeusers_sql['insert_eat_activeusers_daily'], query_data)


def update_eat_active_user_weekly():
    start_date, end_date = DateUtil.get_last_week_date()
    dto = [DateUtil.date2str(start_date, '%Y-%m-%d'), DateUtil.date_to_milli_seconds(start_date),
           DateUtil.date_to_milli_seconds(end_date)]
    query_data = DBCli().huoli_cli.queryOne(eat_activeusers_sql['eat_activeusers_weekly'], dto)
    DBCli().targetdb_cli.insert(eat_activeusers_sql['insert_eat_activeusers_weekly'], query_data)


def update_eat_active_user_monthly():
    start_date, end_date = DateUtil.get_last_month_date()
    dto = [DateUtil.date2str(start_date, '%Y-%m-%d'), DateUtil.date_to_milli_seconds(start_date),
           DateUtil.date_to_milli_seconds(end_date)]
    query_data = DBCli().huoli_cli.queryOne(eat_activeusers_sql['eat_activeusers_monthly'], dto)
    DBCli().targetdb_cli.insert(eat_activeusers_sql['insert_eat_activeusers_monthly'], query_data)


def update_eat_active_user_quarterly():
    start_date, end_date = DateUtil.get_last_quarter_date()
    dto = [DateUtil.date2str(start_date, '%Y-%m-%d'), DateUtil.date2str(start_date, '%Y-%m-%d'),
           DateUtil.date_to_milli_seconds(start_date), DateUtil.date_to_milli_seconds(end_date)]
    query_data = DBCli().huoli_cli.queryOne(eat_activeusers_sql['eat_activeusers_quarterly'], dto)
    DBCli().targetdb_cli.insert(eat_activeusers_sql['insert_eat_activeusers_quarterly'], query_data)

if __name__ == "__main__":
    update_eat_active_user_daily(1)
    # update_eat_active_user_weekly()
    # update_eat_active_user_monthly()