from dbClient.dateutil import DateUtil
from dbClient.db_client import DBCli
from sql.huoli_sqlHandlers import hotel_orders_sql


def update_hotel_orders_daily(days=0):
    # start_date = DateUtil.get_date_before_days(days)
    # end_date = DateUtil.get_date_after_days(1 - int(days))

    if days > 0:
        start_date = DateUtil.date2str(DateUtil.get_date_before_days(3), '%Y-%m-%d')
        end_date = DateUtil.date2str(DateUtil.get_date_after_days(0), '%Y-%m-%d')
    else:
        end_date = DateUtil.date2str(DateUtil.get_date_after_days(1), '%Y-%m-%d')
        start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')

    dto = [start_date, end_date, start_date, end_date]
    query_data = DBCli().sky_hotel_cli.queryAll(hotel_orders_sql["hotel_orders_daily"], dto)
    DBCli().targetdb_cli.batchInsert(hotel_orders_sql["update_hotel_orders_daily"], query_data)

if __name__ == "__main__":
    update_hotel_orders_daily(2)
