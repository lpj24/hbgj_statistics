from dbClient.dateutil import DateUtil
from dbClient.db_client import DBCli
from sql.huoli_sqlHandlers import hotel_consumers_sql


def update_hotel_consumers_daily(days=0):
    start_date = DateUtil.getDateBeforeDays(days)
    end_date = DateUtil.getDateAfterDays(1 - int(days))
    dto = [DateUtil.date2str(start_date), DateUtil.date2str(end_date),
           DateUtil.date2str(start_date), DateUtil.date2str(end_date)]
    query_data = DBCli().sky_hotel_cli.queryOne(hotel_consumers_sql["hotel_consumers_daily"], dto)
    DBCli().targetdb_cli.insert(hotel_consumers_sql["update_hotel_consumers_daily"], query_data)


def update_hotel_consumers_weekly():
    start_date, end_date = DateUtil.getLastWeekDate()
    dto = [DateUtil.date2str(start_date), DateUtil.date2str(end_date),
           DateUtil.date2str(start_date), DateUtil.date2str(end_date)]
    query_data = DBCli().sky_hotel_cli.queryOne(hotel_consumers_sql["hotel_consumers_weekly"], dto)
    DBCli().targetdb_cli.insert(hotel_consumers_sql["update_hotel_consumers_weekly"], query_data)


def update_hotel_consumers_monthly():
    start_date, end_date = DateUtil.getLastMonthDate()
    dto = [DateUtil.date2str(start_date), DateUtil.date2str(end_date),
           DateUtil.date2str(start_date), DateUtil.date2str(end_date)]
    query_data = DBCli().sky_hotel_cli.queryOne(hotel_consumers_sql["hotel_consumers_monthly"], dto)
    DBCli().targetdb_cli.insert(hotel_consumers_sql["update_hotel_consumers_monthly"], query_data)

if __name__ == "__main__":
    update_hotel_consumers_daily(1)
    # update_hotel_consumers_weekly()
    # update_hotel_consumers_monthly()