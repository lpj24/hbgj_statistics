# -*- coding: utf-8 -*-
from sql.gt_sqlHandlers import gtgj_activeusers_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_gtgj_activeusers_daily(days=0):
    """高铁活跃用户(日), gtgj_activeusers_daily"""
    if days > 0:
        today = DateUtil.date2str(DateUtil.get_date_before_days(3), '%Y-%m-%d')
        tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(0), '%Y-%m-%d')
    else:
        tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(1), '%Y-%m-%d')
        today = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    dto = [tomorrow, today]
    query_data = DBCli().gt_cli.query_all(gtgj_activeusers_sql["gtgj_activeusers_daily"], dto)
    DBCli().targetdb_cli.batch_insert(gtgj_activeusers_sql["update_gtgj_activeusers_daily"], query_data)

    wechat_gt_sql = """
        select visit_uv, DATE_FORMAT(ref_date, '%%Y-%%m-%%d') s_day from applet_visit_trend where DATE_FORMAT(ref_date, '%%Y-%%m-%%d') < %s
        and DATE_FORMAT(ref_date, '%%Y-%%m-%%d') >= %s and trend_type=1
    """

    wechat_uv = DBCli().gt_wechat_cli.query_all(wechat_gt_sql, dto)

    update_wechat_sql = """
        update gtgj_activeusers_daily set weixin_users=%s where s_day=%s
    """
    DBCli().targetdb_cli.batch_insert(update_wechat_sql, wechat_uv)


def update_gtgj_activeusers_weekly():
    """高铁活跃用户周, gtgj_activeusers_weekly"""
    start_date = DateUtil.get_last_week_date(DateUtil.get_last_week_date()[0])[0]
    end_date = DateUtil.get_last_week_date()[1]
    dto = [start_date, end_date]
    query_data = DBCli().gt_cli.query_all(gtgj_activeusers_sql["gtgj_activeusers_weekly"], dto)
    DBCli().targetdb_cli.batch_insert(gtgj_activeusers_sql["update_gtgj_activeusers_weekly"], query_data)

    wechat_gt_sql = """
        select visit_uv, DATE_FORMAT(SUBSTR(ref_date, 1, 8), '%%Y-%%m-%%d') s_day from applet_visit_trend where trend_type=2 
        and DATE_FORMAT(SUBSTR(ref_date, 1, 8), '%%Y-%%m-%%d') >= %s
        and DATE_FORMAT(SUBSTR(ref_date, 1, 8), '%%Y-%%m-%%d') < %s
    """

    wechat_uv = DBCli().gt_wechat_cli.query_all(wechat_gt_sql, dto)

    update_wechat_sql = """
        update gtgj_activeusers_weekly set weixin_users=%s where s_day=%s
    """
    DBCli().targetdb_cli.batch_insert(update_wechat_sql, wechat_uv)


def update_gtgj_activeusers_monthly():
    start_date, end_date = DateUtil.get_last_month_date()
    dto = [start_date, end_date]
    query_data = DBCli().gt_cli.query_one(gtgj_activeusers_sql["gtgj_activeusers_monthly"], dto)
    DBCli().targetdb_cli.insert(gtgj_activeusers_sql["update_gtgj_activeusers_monthly"], query_data)

    query_month_day = DateUtil.date2str(start_date, '%Y-%m-%d')
    year, month, _ = query_month_day.split("-")

    query_month_dto = [start_date, year + month]
    wechat_gt_sql = """
        select visit_uv, %s s_day from applet_visit_trend where trend_type=3 
        and SUBSTR(ref_date, 1, 6) = %s 
    """

    wechat_uv = DBCli().gt_wechat_cli.query_all(wechat_gt_sql, query_month_dto)

    update_wechat_sql = """
        update gtgj_activeusers_monthly set weixin_users=%s where s_day=%s
    """
    DBCli().targetdb_cli.batch_insert(update_wechat_sql, wechat_uv)


def update_gtgj_newusers_daily(days=0):
    """高铁新用户(日), gtgj_newusers_daily"""
    if days > 0:
        today = DateUtil.date2str(DateUtil.get_date_before_days(3), '%Y-%m-%d')
        tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(0), '%Y-%m-%d')
    else:
        tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(1), '%Y-%m-%d')
        today = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    dto = [tomorrow, today]
    query_data = DBCli().gt_cli.query_all(gtgj_activeusers_sql["gtgj_newusers_daily"], dto)
    DBCli().targetdb_cli.batch_insert(gtgj_activeusers_sql["update_gtgj_newusers_daily"], query_data)

    wechat_gt_sql = """
        select visit_uv_new, DATE_FORMAT(ref_date, '%%Y-%%m-%%d') s_day from applet_visit_trend where DATE_FORMAT(ref_date, '%%Y-%%m-%%d') < %s
        and DATE_FORMAT(ref_date, '%%Y-%%m-%%d') >= %s and trend_type=1
    """

    wechat_uv = DBCli().gt_wechat_cli.query_all(wechat_gt_sql, dto)

    update_wechat_sql = """
        update gtgj_newusers_daily set new_users_weixin=%s where s_day=%s
    """
    DBCli().targetdb_cli.batch_insert(update_wechat_sql, wechat_uv)


def update_gtgj_activeusers_quarterly():
    # import datetime
    # start_date, end_date = DateUtil.get_last_quarter_date()
    # s_day = str(start_date.year) + ",Q" + str(DateUtil.get_quarter_by_month(start_date.month))
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
    # start_date, end_date = DateUtil.get_last_month_date()
    # key_str = DateUtil.date2str(start_date, '%Y%m%d')
    # start_date = DateUtil.date2str(start_date)
    # end_date = DateUtil.date2str(end_date)
    # dto = [start_date, end_date]
    # uids = gt_cli.query_all(gtgj_activeusers_month_uids, dto)
    # uid_key = "Gt_"+key_str+"_month_uids"
    # for uid in uids:
    #     redis_cli.sadd(uid_key, uid)
    pass




if __name__ == "__main__":
    #下面2项只在凌晨前三天
    # import datetime
    # import time
    # start_date = datetime.date(2018, 6, 1)
    # while 1:
    #     update_gtgj_activeusers_monthly(start_date)
    #     start_date, _ = DateUtil.get_last_month_date(start_date)
    # update_gtgj_activeusers_monthly()
    # update_gtgj_newusers_daily(1)
    # update_gtgj_activeusers_weekly()
    # update_gtgj_activeusers_quarterly()
    update_gtgj_activeusers_daily(1)
