# -*- coding: utf-8 -*-
from dbClient.dateutil import DateUtil
from dbClient.db_client import DBCli
from sql.huoli_sqlHandlers import hotel_orders_sql


def update_hotel_orders_daily(days=0):
    """酒店订单(日), hotel_orders_daily"""
    if days > 0:
        start_date = DateUtil.date2str(DateUtil.get_date_before_days(7), '%Y-%m-%d')
        end_date = DateUtil.date2str(DateUtil.get_date_after_days(0), '%Y-%m-%d')
    else:
        end_date = DateUtil.date2str(DateUtil.get_date_after_days(1), '%Y-%m-%d')
        start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')

    dto = [start_date, end_date, start_date, end_date]
    query_data = DBCli().tongji_skyhotel_cli.query_all(hotel_orders_sql["hotel_orders_daily"], dto)
    DBCli().targetdb_cli.batch_insert(hotel_orders_sql["update_hotel_orders_daily"], query_data)
    pass


def update_hotel_roomcount_channel_daily(days=0):
    """更新酒店渠道间夜数, hotel_roomcount_channel_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(days - 1), '%Y-%m-%d')
    sql = """
        select %s,
        gdsid channel,
        count(*) order_count, 
        sum((to_days(leavedate) - to_days(arrivedate)) * roomcount) room_count
        from hotelorder 
        where arrivedate >= %s
        and leavedate < %s
        and gdsdesc in ("已确认","已入住","已结账")
        group by channel
    """

    insert_sql = """
        insert into hotel_roomcount_channel_daily (s_day, channel, order_count, room_count, create_time, update_time)
        values (%s, %s, %s, %s, now(), now())
        on duplicate key update update_time = now() ,
        s_day = values(s_day),
        channel = values(channel),
        order_count = values(order_count),
        room_count = values(room_count)
    """
    dto = [start_date, start_date, end_date]
    hotel_result = DBCli().tongji_skyhotel_cli.query_all(sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, hotel_result)


if __name__ == "__main__":
    i = 1
    while 1:
        update_hotel_roomcount_channel_daily(i)
        i += 1
