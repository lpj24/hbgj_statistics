# -*- coding: utf-8 -*-
from dbClient.dateutil import DateUtil
from dbClient.db_client import sky_hotel_cli, targetdb_cli, sourcedb_cli, redis_cli
from sql.huoli_sqlHandlers import hotel_newconsumers_sql
import datetime


def update_hotel_newconsumers_daily_history():
    # start_date = datetime.date(2016, 3, 3)
    # end_date = datetime.date(2016, 7, 6)
    start_date = datetime.date(2012, 9, 11)
    end_date = datetime.date(2016, 4, 20)
    while start_date <= end_date:
        tomorrow = DateUtil.add_days(start_date, 1)
        dto = [DateUtil.date2str(start_date, '%Y-%m-%d'), DateUtil.date2str(start_date),
               DateUtil.date2str(tomorrow), DateUtil.date2str(start_date)]

        # query_data = sky_hotel_cli.queryOne(hotel_newconsumers_sql["hotel_newconsumers_daily"], dto)
        query_data = sourcedb_cli.queryOne(hotel_newconsumers_sql["hotel_newconsumers_daily"], dto)
        targetdb_cli.insert(hotel_newconsumers_sql["update_hotel_newconsumers_daily"], query_data)
        start_date = DateUtil.add_days(start_date, 1)


def get_uids_history():
    sql = """
        SELECT distinct phoneid
        from hotelorder
        where createtime<'2016-04-21 00:00:00'
        and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
    """
    query_data = sourcedb_cli.queryAll(sql)
    for uid in query_data:
        redis_cli.sadd("hotel_phoneid_history", uid[0])


def update_new_hotel_newconsumers_daily_history():
    start_date = datetime.date(2016, 7, 7)
    end_date = datetime.date(2016, 7, 10)
    while start_date <= end_date:
        tomorrow = DateUtil.add_days(start_date, 1)
        dto = [DateUtil.date2str(start_date),
               DateUtil.date2str(tomorrow), DateUtil.date2str(start_date)]

        query_data = sky_hotel_cli.queryAll(hotel_newconsumers_sql["hotel_newconsumers_daily"], dto)
        p2p_num = sky_hotel_cli.queryOne(hotel_newconsumers_sql["hotel_newconsumers_p2p_daily"], dto)
        for uid in query_data:
            redis_cli.sadd("hotel_newconsumers_day", uid[0])
        newconsumers_num = redis_cli.sdiffstore("hotel_newconsumers_day", "hotel_newconsumers_day", "hotel_phoneid_history")
        insert_data = (DateUtil.date2str(start_date, '%Y-%m-%d'), newconsumers_num, p2p_num[0])
        targetdb_cli.insert(hotel_newconsumers_sql["update_hotel_newconsumers_daily"], insert_data)
        redis_cli.delete("hotel_newconsumers_day")
        start_date = DateUtil.add_days(start_date, 1)


if __name__ == "__main__":
    # update_hotel_newconsumers_daily_history()
    # get_uids_history()
    update_new_hotel_newconsumers_daily_history()