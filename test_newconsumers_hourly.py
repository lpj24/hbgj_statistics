import datetime
from dbClient.dateutil import DateUtil
from dbClient.db_client import targetdb_cli, gt_cli
import redis


if __name__ == "__main__":

    s_day = DateUtil.getToday("%Y-%m-%d")
    s_hour = int(datetime.datetime.now().strftime("%H"))

    s_day = "2016-06-13"
    s_hour = 10
    if s_hour == 0:
        s_day = DateUtil.date2str(DateUtil.getDateBeforeDays(1), '%Y-%m-%d')
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
    query_data_ios = gt_cli.queryAll(hourly_sql_ios, dto)
    query_data_android = gt_cli.queryAll(hourly_sql_android, dto)

    res_ios = map(lambda x: x[0], query_data_ios)
    res_android = map(lambda x: x[0], query_data_android)

    redis_cli = redis.Redis(host='127.0.0.1', port=6379, db=1)

    for uid in res_ios:
        redis_cli.sadd("hour_uid_ios", uid)

    for uid in res_android:
        redis_cli.sadd("hour_uid_android", uid)

    redis_cli.sdiffstore("hour_uid_ios", "hour_uid_ios", "total_uids_ios")
    redis_cli.sdiffstore("hour_uid_ios", "hour_uid_ios", "total_uids_android")
    redis_cli.sdiffstore("hour_uid_ios", "hour_uid_ios", "total_uids")
    hour_uids_ios = redis_cli.sdiffstore("hour_uid_ios", "hour_uid_ios", s_day + "_hour_uids")

    redis_cli.sdiffstore("hour_uid_android", "hour_uid_android", "total_uids_ios")
    redis_cli.sdiffstore("hour_uid_android", "hour_uid_android", "total_uids_android")
    redis_cli.sdiffstore("hour_uid_android", "hour_uid_android", "total_uids")
    hour_uids_android = redis_cli.sdiffstore("hour_uid_android", "hour_uid_android", s_day + "_hour_uids")

    redis_cli.sunionstore(s_day + "_hour_uids", s_day + "_hour_uids", "hour_uid_android")
    redis_cli.sunionstore(s_day + "_hour_uids", s_day + "_hour_uids", "hour_uid_ios")
    redis_cli.expire(s_day + "_hour_uids", 86400)
    redis_cli.delete("hour_uid_android")
    redis_cli.delete("hour_uid_ios")

    update_new_consumers_hourly_sql = """
        insert into gtgj_newconsumers_hourly (hour, s_day, new_consumers, new_consumers_ios,
        new_consumers_android, createtime, updatetime) values (%s, %s, %s, %s , %s, now(), now())
    """

    dto = [s_hour, s_day, hour_uids_ios+hour_uids_android, hour_uids_ios, hour_uids_android]
    targetdb_cli.insert(update_new_consumers_hourly_sql, dto)


