from dbClient.dateutil import DateUtil
from dbClient.db_client import DBCli
from sql.huoli_sqlHandlers import hotel_orders_sql, hotel_consumers_sql
import datetime


def update_hotel_orders_daily():
    start_date = datetime.date(2016, 4, 21)
    # end_date = DateUtil.get_date_after_days(1 - int(days))
    end_date = datetime.date(2016, 7, 14)
    # dto = [DateUtil.date2str(end_date), DateUtil.date2str(end_date)]
    dto = [DateUtil.date2str(start_date), DateUtil.date2str(end_date),
           DateUtil.date2str(start_date), DateUtil.date2str(end_date)]
    # query_data = DBCli().sourcedb_cli.queryAll(hotel_orders_sql["hotel_orders_daily_history"], dto)
    query_data = DBCli().sky_hotel_cli.queryAll(hotel_orders_sql["hotel_orders_daily_history"], dto)
    DBCli().targetdb_cli.batchInsert(hotel_orders_sql["update_hotel_orders_daily"], query_data)


def update_hotel_consumers_daily_his():
    start_date = datetime.date(2016, 4, 21)
    # end_date = DateUtil.get_date_after_days(1 - int(days))
    end_date = datetime.date(2016, 7, 14)
    # dto = [DateUtil.date2str(end_date), DateUtil.date2str(end_date)]
    dto = [DateUtil.date2str(start_date), DateUtil.date2str(end_date),
           DateUtil.date2str(start_date), DateUtil.date2str(end_date)]
    # query_data = DBCli().sourcedb_cli.queryAll(hotel_consumers_sql["hotel_consumers_daily"], dto)
    query_data = DBCli().sky_hotel_cli.queryAll(hotel_consumers_sql["hotel_consumers_daily"], dto)
    DBCli().targetdb_cli.batchInsert(hotel_consumers_sql["update_hotel_consumers_daily"], query_data)


def update_hotel_consumers_weekly_his():
    start_date = datetime.date(2016, 4, 18)
    # end_date = DateUtil.get_date_after_days(1 - int(days))
    end_date = datetime.date(2016, 7, 11)
    # dto = [DateUtil.date2str(end_date), DateUtil.date2str(end_date)]
    dto = [DateUtil.date2str(start_date), DateUtil.date2str(end_date),
           DateUtil.date2str(start_date), DateUtil.date2str(end_date)]
    # query_data = DBCli().sourcedb_cli.queryAll(hotel_consumers_sql["hotel_consumers_weekly_his"], dto)
    query_data = DBCli().sky_hotel_cli.queryAll(hotel_consumers_sql["hotel_consumers_weekly"], dto)
    DBCli().targetdb_cli.batchInsert(hotel_consumers_sql["update_hotel_consumers_weekly"], query_data)


def update_hotel_consumers_monthly_his():
    start_date = datetime.date(2016, 4, 1)
    # end_date = DateUtil.get_date_after_days(1 - int(days))
    end_date = datetime.date(2016, 7, 1)
    # dto = [DateUtil.date2str(end_date), DateUtil.date2str(end_date)]
    dto = [DateUtil.date2str(start_date), DateUtil.date2str(end_date),
           DateUtil.date2str(start_date), DateUtil.date2str(end_date)]
    # query_data = DBCli().sourcedb_cli.queryAll(hotel_consumers_sql["hotel_consumers_monthly_his"], dto)
    query_data = DBCli().sky_hotel_cli.queryAll(hotel_consumers_sql["hotel_consumers_monthly"], dto)
    DBCli().targetdb_cli.batchInsert(hotel_consumers_sql["update_hotel_consumers_monthly"], query_data)

if __name__ == "__main__":
    # update_hotel_consumers_daily_his()
    # update_hotel_consumers_weekly_his()
    update_hotel_consumers_monthly_his()