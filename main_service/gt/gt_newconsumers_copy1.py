import redis
from dbClient.dateutil import DateUtil
from dbClient.db_client import gt_cli, targetdb_cli, redis_cli
import os
import datetime


def gt_newconsumers_history():
    file_list = os.walk("C:\Users\Administrator\PycharmProjects\hbgj_statistics\data")
    # file_list = os.walk("/home/huolibi/task/gt_newconsumers/data")
    for dirpath, dirnames, filenames in file_list:
        for f_uid in filenames:
            filename = os.path.join(dirpath, f_uid)
            if filename.endswith("ios"):
                uids_key = "total_uids_ios"
            elif filename.endswith("android"):
                uids_key = "total_uids_android"
            else:
                uids_key = "total_uids"
            with open(filename, 'r') as fp:
                while 1:
                    uid = fp.readline()
                    if uid:
                        redis_cli.sadd(uids_key, uid[0:-1])
                    else:
                        break


def gt_newconsumers_daily(days=0):
    new_consumers_daily_ios = """
            SELECT distinct uid
                  FROM user_order
                  where i_status=3
                  and p_info LIKE '%%ios%%'
                  and pay_time>=%s
                  and pay_time<%s
        """

    new_consumers_daily_android = """
                    SELECT distinct uid
                  FROM user_order
                  where i_status=3
                  and p_info LIKE '%%android%%'
                  and pay_time>=%s
                  and pay_time<%s
        """
    start_date = DateUtil.get_date_before_days(days)
    dto = [DateUtil.date2str(start_date), DateUtil.date2str(DateUtil.add_days(start_date, 1))]
    print dto

    # query_data_ios = gt_cli.query_all(new_consumers_daily_ios, dto)
    # query_data_android = gt_cli.query_all(new_consumers_daily_android, dto)
    # res_ios = map(lambda x: x[0], query_data_ios)
    # res_android = map(lambda x: x[0], query_data_android)
    #
    # for uid_ios in res_ios:
    #     redis_cli.sadd("today_uid_ios", uid_ios)
    #
    # for uid_android in res_android:
    #     redis_cli.sadd("today_uid_android", uid_android)
    #
    # redis_cli.sdiffstore("today_uid_ios", "today_uid_ios", "total_uids_ios")
    # redis_cli.sdiffstore("today_uid_ios", "today_uid_ios", "total_uids_android")
    # today_uids_ios = redis_cli.sdiffstore("today_uid_ios", "today_uid_ios", "total_uids")
    #
    # redis_cli.sdiffstore("today_uid_android", "today_uid_android", "total_uids_android")
    # redis_cli.sdiffstore("today_uid_android", "today_uid_android", "total_uids_ios")
    # today_uids_android = redis_cli.sdiffstore("today_uid_android", "today_uid_android", "total_uids")
    #
    #
    # sql = """ insert into gtgj_newconsumers_daily values (%s, %s, %s , %s, now(), now())
    #     on duplicate key update updatetime = now(),
    #     s_day = VALUES(s_day),
    #     new_consumers = VALUES(new_consumers),
    #     new_consumers_ios = VALUES(new_consumers_ios),
    #     new_consumers_android = VALUES(new_consumers_android)
    #     """
    # dto = [DateUtil.date2str(start_date, '%Y-%m-%d'), today_uids_ios + today_uids_android, today_uids_ios,
    #        today_uids_android]
    # # targetdb_cli.insert(sql, dto)
    # print dto
    # redis_cli.sunionstore("total_uids_ios", "total_uids_ios", "today_uid_ios")
    # redis_cli.sunionstore("total_uids_android", "total_uids_android", "today_uid_android")
    #
    # redis_cli.delete("today_uid_ios")
    # redis_cli.delete("today_uid_android")


def gt_newconsumers_hourly():
    s_day = DateUtil.get_today("%Y-%m-%d")
    s_hour = int(datetime.datetime.now().strftime("%H"))
    s_day = '2016-07-05'
    for s_hour in xrange(8, 15):
        if s_hour == 0:
            s_day = DateUtil.date2str(DateUtil.get_date_before_days(1), '%Y-%m-%d')
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
        query_data_ios = gt_cli.query_all(hourly_sql_ios, dto)
        query_data_android = gt_cli.query_all(hourly_sql_android, dto)

        res_ios = map(lambda x: x[0], query_data_ios)
        res_android = map(lambda x: x[0], query_data_android)

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

        dto = [s_hour, s_day, hour_uids_ios + hour_uids_android, hour_uids_ios, hour_uids_android]
        print dto
        # targetdb_cli.insert(update_new_consumers_hourly_sql, dto)


if __name__ == "__main__":
    # gt_newconsumers_daily(1)
    # gt_newconsumers_history()
    gt_newconsumers_hourly()