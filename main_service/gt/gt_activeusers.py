# -*- coding: utf-8 -*-
from sql.gt_sqlHandlers import gtgj_activeusers_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_gtgj_activeusers_daily(days=0):
    if days > 0:
        today = DateUtil.date2str(DateUtil.getDateBeforeDays(3), '%Y-%m-%d')
        tomorrow = DateUtil.date2str(DateUtil.getDateAfterDays(0), '%Y-%m-%d')
    else:
        tomorrow = DateUtil.date2str(DateUtil.getDateAfterDays(1), '%Y-%m-%d')
        today = DateUtil.date2str(DateUtil.getDateBeforeDays(days), '%Y-%m-%d')
    dto = [tomorrow, today]
    query_data = DBCli().gt_cli.queryAll(gtgj_activeusers_sql["gtgj_activeusers_daily"], dto)
    DBCli().targetdb_cli.batchInsert(gtgj_activeusers_sql["update_gtgj_activeusers_daily"], query_data)


def update_gtgj_activeusers_weekly():
    start_date = DateUtil.getLastWeekDate(DateUtil.getLastWeekDate()[0])[0]
    end_date = DateUtil.getLastWeekDate()[1]
    dto = [start_date, end_date]
    query_data = DBCli().gt_cli.queryAll(gtgj_activeusers_sql["gtgj_activeusers_weekly"], dto)
    DBCli().targetdb_cli.batchInsert(gtgj_activeusers_sql["update_gtgj_activeusers_weekly"], query_data)


def update_gtgj_activeusers_monthly():
    start_date, end_date = DateUtil.getLastMonthDate()
    dto = [start_date, end_date]
    query_data = DBCli().gt_cli.queryOne(gtgj_activeusers_sql["gtgj_activeusers_monthly"], dto)
    DBCli().targetdb_cli.insert(gtgj_activeusers_sql["update_gtgj_activeusers_monthly"], query_data)


def update_gtgj_newusers_daily(days=0):
    if days > 0:
        today = DateUtil.date2str(DateUtil.getDateBeforeDays(3), '%Y-%m-%d')
        tomorrow = DateUtil.date2str(DateUtil.getDateAfterDays(0), '%Y-%m-%d')
    else:
        tomorrow = DateUtil.date2str(DateUtil.getDateAfterDays(1), '%Y-%m-%d')
        today = DateUtil.date2str(DateUtil.getDateBeforeDays(days), '%Y-%m-%d')
    dto = [tomorrow, today]
    query_data = DBCli().gt_cli.queryAll(gtgj_activeusers_sql["gtgj_newusers_daily"], dto)
    DBCli().targetdb_cli.batchInsert(gtgj_activeusers_sql["update_gtgj_newusers_daily"], query_data)


def update_gtgj_activeusers_quarterly():
    # import datetime
    # start_date, end_date = DateUtil.getLastQuarterDate()
    # s_day = str(start_date.year) + ",Q" + str(DateUtil.getQuarterByMonth(start_date.month))
    # while start_date < end_date:
    #     file_name = "Hb_uid" + DateUtil.date2str(start_date, '%Y%m')
    #     print file_name
    #     with open("D:\\"+file_name, 'r') as uid_file:
    #         while 1:
    #             uid = uid_file.readline()
    #             if uid:
    #                 # redis_cli.sadd("gtgj_activeusers_quarterly", uid)
    #                 redis_cli.sadd("hbgj_activeusers_quarterly", uid)
    #             else:
    #                 break
    #     start_date = datetime.date(start_date.year, start_date.month+1, 1)
    # redis_cli.expire("hbgj_activeusers_quarterly", 86400)
    # query_data = [s_day, redis_cli.scard("gtgj_activeusers_quarterly"), 0, 0]
    # targetdb_cli.insert(gtgj_activeusers_sql["update_gtgj_activeusers_quarterly"], query_data)
    # gtgj_activeusers_month_uids = """
    # select distinct uid
    # from user_statistics
    # where end_time>=%s
    # and end_time<%s
    # """
    # start_date, end_date = DateUtil.getLastMonthDate()
    # key_str = DateUtil.date2str(start_date, '%Y%m%d')
    # start_date = DateUtil.date2str(start_date)
    # end_date = DateUtil.date2str(end_date)
    # dto = [start_date, end_date]
    # uids = gt_cli.queryAll(gtgj_activeusers_month_uids, dto)
    # uid_key = "Gt_"+key_str+"_month_uids"
    # for uid in uids:
    #     redis_cli.sadd(uid_key, uid)
    pass




if __name__ == "__main__":
    #下面2项只在凌晨更新前三天
    update_gtgj_activeusers_daily(1)
    update_gtgj_newusers_daily(2)
    update_gtgj_activeusers_weekly()
    update_gtgj_activeusers_quarterly()
