# -*- coding: utf-8 -*-
from dbClient.dateutil import DateUtil
from dbClient.db_client import DBCli


def update_hotel_activeusers_daily(days=0):
    """更新酒店活跃用户(请先更新酒店新用户hotel_newusers_daily), hotel_activeusers_daily"""
    s_day = DateUtil.date2str(DateUtil.get_date_before_days(days), "%Y%m%d")
    uid_key = s_day + "_activeusers"
    activeusers_num = DBCli().redis_cli.scard(uid_key)
    activeusers_daily_sql = """
        insert into hotel_activeusers_daily values (%s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        activeusers = VALUES(activeusers)
    """
    if activeusers_num > 0:
        dto = [DateUtil.date2str(DateUtil.get_date_before_days(days), "%Y-%m-%d"), activeusers_num]
        DBCli().targetdb_cli.insert(activeusers_daily_sql, dto)
    pass


def update_hotel_activeusers_weekly(days=0):
    """更新酒店活跃用户(周), hotel_activeusers_weekly"""
    start_week, end_week = DateUtil.get_last_week_date(DateUtil.get_date_before_days(days))
    start_week = DateUtil.add_days(start_week, 7)
    s_day = start_week
    week_activeusers_num = 0
    today = DateUtil.get_date_before_days(days)
    while start_week <= today:
        week_uid_key = DateUtil.date2str(s_day, "%Y%m%d") + "_week_activeusers"
        week_activeusers_num = DBCli().redis_cli.sunionstore(week_uid_key, DateUtil.date2str(start_week, '%Y%m%d')+"_activeusers",
                                                     week_uid_key)
        start_week = DateUtil.add_days(start_week, 1)

    DBCli().redis_cli.expire(week_uid_key, 86400)

    activeuser_weekly_sql = """
        insert into hotel_activeusers_weekly values (%s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        activeusers = VALUES(activeusers)
    """
    dto = [DateUtil.date2str(s_day, "%Y-%m-%d"), week_activeusers_num]
    DBCli().targetdb_cli.insert(activeuser_weekly_sql, dto)
    pass


def update_hotel_activeusers_monthly():
    start_month, end_month = DateUtil.get_last_month_date()
    s_day = start_month
    month_activeusers_num = 0
    while start_month < end_month:
        month_uid_key = DateUtil.date2str(s_day, "%Y%m%d") + "_month_activeusers"
        month_activeusers_num = DBCli().redis_cli.sunionstore(month_uid_key,
                                                      DateUtil.date2str(start_month, '%Y%m%d') + "_activeusers",
                                                      month_uid_key)
        start_month = DateUtil.add_days(start_month, 1)

    activeuser_monthly_sql = """
        insert into hotel_activeusers_monthly values (%s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        activeusers = VALUES(activeusers)
    """
    dto = [DateUtil.date2str(s_day, "%Y-%m-%d"), month_activeusers_num]
    DBCli().targetdb_cli.insert(activeuser_monthly_sql, dto)


if __name__ == "__main__":
    update_hotel_activeusers_daily(1)
    # update_hotel_activeusers_weekly(1)
    # update_hotel_activeusers_monthly()





