from dbClient.dateutil import DateUtil
from dbClient.db_client import DBCli
import os
import datetime


def gt_newconsumers_hourly():
    s_day = DateUtil.getToday("%Y-%m-%d")
    s_hour = int(datetime.datetime.now().strftime("%H"))
    s_day = '2016-07-01'
    s_hour = 0
    if s_hour == 0:
        # s_day = DateUtil.date2str(DateUtil.getDateBeforeDays(1), '%Y-%m-%d')
        s_hour = 23
    else:
        s_hour -= 1

    query_start_date = s_day + " " + str(s_hour) + ":00:00"
    query_end_date = s_day + " " + str(s_hour) + ":59:59"
    dto = [query_start_date, query_end_date]
    print dto
    hourly_sql_ios = """
        SELECT distinct uid
                  FROM user_order
                  where i_status=3
                  and p_info LIKE '%%ios%%'
                  and pay_time>=%s
                  and pay_time<=%s

        """

    hourly_sql_android = """
        SELECT distinct uid
                  FROM user_order
                  where i_status=3
                  and p_info LIKE '%%android%%'
                  and pay_time>=%s
                  and pay_time<=%s

        """
    query_data_ios = DBCli().gt_cli.queryAll(hourly_sql_ios, dto)
    query_data_android = DBCli().gt_cli.queryAll(hourly_sql_android, dto)

    res_ios = map(lambda x: x[0], query_data_ios)
    res_android = map(lambda x: x[0], query_data_android)

    for uid in res_ios:
        DBCli().redis_cli.sadd("hour_uid_ios", uid)

    for uid in res_android:
        DBCli().redis_cli.sadd("hour_uid_android", uid)

    DBCli().redis_cli.sdiffstore("hour_uid_ios", "hour_uid_ios", "total_uids_ios")
    DBCli().redis_cli.sdiffstore("hour_uid_ios", "hour_uid_ios", "total_uids_android")
    DBCli().redis_cli.sdiffstore("hour_uid_ios", "hour_uid_ios", "total_uids")
    hour_uids_ios = DBCli().redis_cli.sdiffstore("hour_uid_ios", "hour_uid_ios", s_day + "_hour_uids")

    DBCli().redis_cli.sdiffstore("hour_uid_android", "hour_uid_android", "total_uids_ios")
    DBCli().redis_cli.sdiffstore("hour_uid_android", "hour_uid_android", "total_uids_android")
    DBCli().redis_cli.sdiffstore("hour_uid_android", "hour_uid_android", "total_uids")
    hour_uids_android = DBCli().redis_cli.sdiffstore("hour_uid_android", "hour_uid_android", s_day + "_hour_uids")

    DBCli().redis_cli.sunionstore(s_day + "_hour_uids", s_day + "_hour_uids", "hour_uid_android")
    DBCli().redis_cli.sunionstore(s_day + "_hour_uids", s_day + "_hour_uids", "hour_uid_ios")
    DBCli().redis_cli.expire(s_day + "_hour_uids", 86400)
    DBCli().redis_cli.delete("hour_uid_android")
    DBCli().redis_cli.delete("hour_uid_ios")

    update_new_consumers_hourly_sql = """
            insert into gtgj_newconsumers_hourly (hour, s_day, new_consumers, new_consumers_ios,
            new_consumers_android, createtime, updatetime) values (%s, %s, %s, %s , %s, now(), now())
        """

    dto = [s_hour, s_day, hour_uids_ios + hour_uids_android, hour_uids_ios, hour_uids_android]
    DBCli().targetdb_cli.insert(update_new_consumers_hourly_sql, dto)

if __name__ == "__main__":
    gt_newconsumers_hourly()