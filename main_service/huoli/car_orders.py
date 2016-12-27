from sql.huoli_sqlHandlers import car_orders_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_car_orders_daily(days=0):
    today = DateUtil.getDateBeforeDays(int(days))
    tomorrow = DateUtil.getDateAfterDays(1-int(days))
    dto = []
    # for i in xrange(4):
    #     dto.append(DateUtil.date2str(today, '%Y-%m-%d'))
    #     dto.append(DateUtil.date2str(today))
    #     dto.append(DateUtil.date2str(tomorrow))
    # query_data = car_cli.queryOne(car_orders_sql['car_orders_daily'], dto)
    # targetdb_cli.insert(car_orders_sql['update_car_orders_daily'], query_data)

    #jiesongji jisongzhan
    for i in xrange(2):
        dto.append(DateUtil.date2str(today, '%Y-%m-%d'))
        dto.append(DateUtil.date2str(today))
        dto.append(DateUtil.date2str(tomorrow))
    query_data = DBCli().car_cli.queryOne(car_orders_sql["car_orders_jz_daily"], dto)
    print query_data
    DBCli().targetdb_cli.insert(car_orders_sql["update_car_orders_jz_daily"], query_data)

if __name__ == "__main__":
    # for i in xrange(4, 0, -1):
    #     update_car_orders_daily(i)
    update_car_orders_daily(1)