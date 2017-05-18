# -*- coding: utf-8 -*-
from sql.huoli_sqlHandlers import car_orders_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_car_orders_daily(days=0):
    """更新伙力专车的日订单, huoli_car_orders_daily"""

    today = DateUtil.get_date_before_days(int(days))
    tomorrow = DateUtil.get_date_after_days(1-int(days))
    dto = []
    #jiesongji jisongzhan
    for i in xrange(2):
        dto.append(DateUtil.date2str(today, '%Y-%m-%d'))
        dto.append(DateUtil.date2str(today))
        dto.append(DateUtil.date2str(tomorrow))
    query_data = DBCli().car_cli.queryOne(car_orders_sql["car_orders_jz_daily"], dto)
    DBCli().targetdb_cli.insert(car_orders_sql["update_car_orders_jz_daily"], query_data)

    return __file__

if __name__ == "__main__":
    # for i in xrange(4, 0, -1):
    #     update_car_orders_daily(i)
    i = 15
    while i >= 1:
        update_car_orders_daily(i)
        i -= 1