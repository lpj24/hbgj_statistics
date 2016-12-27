from dbClient.dateutil import DateUtil
from dbClient.db_client import DBCli
from sql.huoli_sqlHandlers import hotel_newconsumers_sql


def update_hotel_newconsumers_daily(days=0):
    start_date = DateUtil.getDateBeforeDays(days)
    end_date = DateUtil.getDateAfterDays(1 - int(days))
    dto = [DateUtil.date2str(start_date),
           DateUtil.date2str(end_date), DateUtil.date2str(start_date)]
    query_data = DBCli().sky_hotel_cli.queryAll(hotel_newconsumers_sql["hotel_newconsumers_daily"], dto)
    p2p_num = DBCli().sky_hotel_cli.queryOne(hotel_newconsumers_sql["hotel_newconsumers_p2p_daily"], dto)
    for uid in query_data:
        DBCli().redis_cli.sadd("hotel_newconsumers_day", uid[0])
    newconsumers_num = DBCli().redis_cli.sdiffstore("hotel_newconsumers_day", "hotel_newconsumers_day", "hotel_phoneid_history")
    DBCli().redis_cli.delete("hotel_newconsumers_day")
    insert_data = (DateUtil.date2str(start_date, '%Y-%m-%d'), newconsumers_num, p2p_num[0])
    DBCli().targetdb_cli.insert(hotel_newconsumers_sql["update_hotel_newconsumers_daily"], insert_data)

if __name__ == "__main__":
    update_hotel_newconsumers_daily(1)
