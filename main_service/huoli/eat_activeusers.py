from sql.huoli_sqlHandlers import eat_activeusers_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_eat_active_user_daily(days=0):
    today = DateUtil.getDateBeforeDays(int(days))
    tomorrow = DateUtil.getDateAfterDays(1-int(days))
    dto = [DateUtil.date2str(today, '%Y-%m-%d'), DateUtil.dateToMilliseconds(today),
           DateUtil.dateToMilliseconds(tomorrow)]
    query_data = DBCli().huoli_cli.queryOne(eat_activeusers_sql['eat_activeusers_daily'], dto)
    DBCli().targetdb_cli.insert(eat_activeusers_sql['insert_eat_activeusers_daily'], query_data)


def update_eat_active_user_weekly():
    start_date, end_date = DateUtil.getLastWeekDate()
    dto = [DateUtil.date2str(start_date, '%Y-%m-%d'), DateUtil.dateToMilliseconds(start_date),
           DateUtil.dateToMilliseconds(end_date)]
    query_data = DBCli().huoli_cli.queryOne(eat_activeusers_sql['eat_activeusers_weekly'], dto)
    DBCli().targetdb_cli.insert(eat_activeusers_sql['insert_eat_activeusers_weekly'], query_data)


def update_eat_active_user_monthly():
    start_date, end_date = DateUtil.getLastMonthDate()
    dto = [DateUtil.date2str(start_date, '%Y-%m-%d'), DateUtil.dateToMilliseconds(start_date),
           DateUtil.dateToMilliseconds(end_date)]
    query_data = DBCli().huoli_cli.queryOne(eat_activeusers_sql['eat_activeusers_monthly'], dto)
    DBCli().targetdb_cli.insert(eat_activeusers_sql['insert_eat_activeusers_monthly'], query_data)


def update_eat_active_user_quarterly():
    start_date, end_date = DateUtil.getLastQuarterDate()
    dto = [DateUtil.date2str(start_date, '%Y-%m-%d'), DateUtil.date2str(start_date, '%Y-%m-%d'),
           DateUtil.dateToMilliseconds(start_date), DateUtil.dateToMilliseconds(end_date)]
    query_data = DBCli().huoli_cli.queryOne(eat_activeusers_sql['eat_activeusers_quarterly'], dto)
    DBCli().targetdb_cli.insert(eat_activeusers_sql['insert_eat_activeusers_quarterly'], query_data)

if __name__ == "__main__":
    update_eat_active_user_daily(2)
    # update_eat_active_user_weekly()
    # update_eat_active_user_monthly()