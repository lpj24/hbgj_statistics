# -*- coding: utf-8 -*-
from sql.hb_sqlHandlers import hb_activeusers_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_hbgj_activeusers_daily(days=0):
    """航班活跃用户(日), hbgj_activeusers_daily"""
    today = DateUtil.date2str(DateUtil.get_date_before_days(int(days) * 3), '%Y-%m-%d')

    tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)), '%Y-%m-%d')
    dto = [today, tomorrow]
    query_data = DBCli().apibase_cli.query_all(hb_activeusers_sql["hbgj_activeusers_daily"], dto)
    DBCli().targetdb_cli.batch_insert(hb_activeusers_sql["update_hbgj_activeusers_daily"], query_data)

    wechat_hb_sql = """
        select visit_uv, DATE_FORMAT(ref_date, '%%Y-%%m-%%d') s_day from AU_HBGJ_APPLET_VISIT where DATE_FORMAT(ref_date, '%%Y-%%m-%%d') < %s
        and DATE_FORMAT(ref_date, '%%Y-%%m-%%d') >= %s and trend_type=1
    """

    wechat_uv = DBCli().source_wechat_cli.query_all(wechat_hb_sql, [tomorrow, today])

    update_wechat_sql = """
        update hbgj_activeusers_daily set active_users_weixin=%s where s_day=%s
    """
    DBCli().targetdb_cli.batch_insert(update_wechat_sql, wechat_uv)


def update_hbgj_activeusers_weekly():
    """航班活跃用户周, hbgj_activeusers_weekly"""
    start_date, end_date = DateUtil.get_last_week_date()
    start_date = DateUtil.date2str(start_date, '%Y-%m-%d')
    end_date = DateUtil.date2str(end_date, '%Y-%m-%d')
    dto = [start_date, end_date]
    query_data = DBCli().apibase_cli.query_one(hb_activeusers_sql["hbgj_activeusers_weekly"], dto)
    DBCli().targetdb_cli.insert(hb_activeusers_sql["update_hbgj_activeusers_weekly"], query_data)

    wechat_hb_sql = """
        select visit_uv, DATE_FORMAT(SUBSTR(ref_date, 1, 8), '%%Y-%%m-%%d') s_day from AU_HBGJ_APPLET_VISIT where trend_type=2 
        and DATE_FORMAT(SUBSTR(ref_date, 1, 8), '%%Y-%%m-%%d') >= %s
        and DATE_FORMAT(SUBSTR(ref_date, 1, 8), '%%Y-%%m-%%d') < %s
    """

    wechat_uv = DBCli().source_wechat_cli.query_all(wechat_hb_sql, dto)

    update_wechat_sql = """
        update hbgj_activeusers_weekly set active_users_weixin=%s where s_day=%s
    """
    DBCli().targetdb_cli.batch_insert(update_wechat_sql, wechat_uv)


def update_hbgj_activeusers_monthly():
    start_date, end_date = DateUtil.get_last_month_date()
    start_date = DateUtil.date2str(start_date, '%Y-%m-%d')
    end_date = DateUtil.date2str(end_date, '%Y-%m-%d')
    dto = [start_date, end_date]
    query_data = DBCli().apibase_cli.query_one(hb_activeusers_sql["hbgj_activeusers_monthly"], dto)
    DBCli().targetdb_cli.insert(hb_activeusers_sql["update_hbgj_activeusers_monthly"], query_data)

    year, month, _ = start_date.split("-")

    query_month_dto = [start_date, year + month]
    wechat_hb_sql = """
            select visit_uv, %s s_day from AU_HBGJ_APPLET_VISIT where trend_type=3 
            and SUBSTR(ref_date, 1, 6) = %s 
        """

    wechat_uv = DBCli().source_wechat_cli.query_all(wechat_hb_sql, query_month_dto)

    update_wechat_sql = """
            update hbgj_activeusers_monthly set active_users_weixin=%s where s_day=%s
        """
    DBCli().targetdb_cli.batch_insert(update_wechat_sql, wechat_uv)


def update_hbgj_newuser_daily(days=1):
    """航班新用户, hbgj_newusers_daily"""
    today = DateUtil.date2str(DateUtil.get_date_before_days(int(days) * 3), '%Y-%m-%d')
    tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)), '%Y-%m-%d')
    dto = [today, tomorrow]
    new_user_sql = """
    select DATE_FORMAT(USER_CREATEDATE,'%%Y-%%m-%%d') s_day,
    COUNT(1) NEW_USER,
    COUNT(case when USER_CHANNEL LIKE '%%91ZS%%' or USER_CHANNEL LIKE '%%appstore%%' or USER_CHANNEL LIKE '%%juwan%%' or USER_CHANNEL LIKE '%%91PGZS%%'
    or USER_CHANNEL LIKE '%%kuaiyong%%' or USER_CHANNEL LIKE '%%TBT%%' or USER_CHANNEL LIKE '%%PPZS%%' THEN 1 END) ios_newuser,
    COUNT(case when USER_CHANNEL not LIKE '%%91ZS%%' and USER_CHANNEL not LIKE '%%appstore%%' and USER_CHANNEL not LIKE '%%juwan%%' AND USER_CHANNEL NOT LIKE '%%91PGZS%%'
    AND USER_CHANNEL NOT LIKE '%%kuaiyong%%' AND USER_CHANNEL NOT LIKE '%%TBT%%' AND USER_CHANNEL NOT LIKE '%%PPZS%%' THEN 1 END) android_newuser
    from HBZJ_USER
    where USER_CREATEDATE>=%s
    and USER_CREATEDATE<%s
    group by s_day
    ORDER BY NEW_USER desc
    """

    insert_sql = """
        insert into hbgj_newusers_daily (s_day, new_users, new_users_ios, new_users_android, new_users_weixin,
        createtime, updatetime) values (%s, %s, %s, %s, 0, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        new_users = values(new_users),
        new_users_ios = values(new_users_ios),
        new_users_android = values(new_users_android),
        new_users_weixin = values(new_users_weixin)
    """
    query_data = DBCli().apibase_cli.query_all(new_user_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)

    wechat_hb_sql = """
        select visit_uv_new, DATE_FORMAT(ref_date, '%%Y-%%m-%%d') s_day from AU_HBGJ_APPLET_VISIT where DATE_FORMAT(ref_date, '%%Y-%%m-%%d') < %s
        and DATE_FORMAT(ref_date, '%%Y-%%m-%%d') >= %s and trend_type=1
    """

    wechat_uv = DBCli().source_wechat_cli.query_all(wechat_hb_sql, [tomorrow, today])

    update_wechat_sql = """
        update hbgj_newusers_daily set new_users_weixin=%s where s_day=%s
    """
    DBCli().targetdb_cli.batch_insert(update_wechat_sql, wechat_uv)


if __name__ == "__main__":
    # for x in xrange(6, 0, -1):
    # i = 5
    # while i >= 1:
    #     update_hbgj_activeusers_daily(i)
    #     i -= 1
    update_hbgj_activeusers_daily(1)
    # import datetime
    # import time
    # start_date = datetime.date(2018, 7, 11)
    # while 1:
    #     start_date, end_date = DateUtil.get_last_month_date(start_date)
    #     update_hbgj_activeusers_monthly(start_date, end_date)
    # update_hbgj_activeusers_weekly()
    # i = 1
    # while i <= 369:
    #     update_hbgj_newuser_daily(i)
    #     i += 1