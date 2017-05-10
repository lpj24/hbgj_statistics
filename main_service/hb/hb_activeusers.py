# -*- coding: utf-8 -*-
from sql.hb_sqlHandlers import hb_activeusers_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_hbgj_activeusers_daily(days=0):
    """更新航班活跃用户(日), hbgj_activeusers_daily"""
    today = DateUtil.date2str(DateUtil.get_date_before_days(int(days)), '%Y-%m-%d')
    tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)), '%Y-%m-%d')
    dto = [today, today, tomorrow]
    query_data = DBCli().apibase_cli.queryOne(hb_activeusers_sql["hbgj_activeusers_daily"], dto)
    DBCli().targetdb_cli.insert(hb_activeusers_sql["update_hbgj_activeusers_daily"], query_data)
    return __file__


def update_hbgj_activeusers_weekly():
    start_date, end_date = DateUtil.get_last_week_date()
    start_date = DateUtil.date2str(start_date, '%Y-%m-%d')
    end_date = DateUtil.date2str(end_date, '%Y-%m-%d')
    dto = {"start_date": start_date, "end_date": end_date}
    query_data = DBCli().oracle_cli.queryOne(hb_activeusers_sql["hbgj_activeusers_weekly"], dto)
    DBCli().targetdb_cli.insert(hb_activeusers_sql["update_hbgj_activeusers_weekly"], query_data)


def update_hbgj_activeusers_monthly():
    start_date, end_date = DateUtil.get_last_month_date()
    start_date = DateUtil.date2str(start_date, '%Y-%m-%d')
    end_date = DateUtil.date2str(end_date, '%Y-%m-%d')
    dto = {"start_date": start_date, "end_date": end_date}
    query_data = DBCli().oracle_cli.queryOne(hb_activeusers_sql["hbgj_activeusers_monthly"], dto)
    DBCli().targetdb_cli.insert(hb_activeusers_sql["update_hbgj_activeusers_monthly"], query_data)


def update_hbgj_newuser_daily(days=1):
    """更新航班新用户, hbgj_newuser_daily"""
    today = DateUtil.date2str(DateUtil.get_date_before_days(int(days)))
    tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)))
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
        insert into hbgj_newusers_daily (s_day, new_users, new_users_ios, new_users_android,
        createtime, updatetime) values (%s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        new_users = values(new_users),
        new_users_ios = values(new_users_ios),
        new_users_android = values(new_users_android)
    """
    query_data = DBCli().apibase_cli.queryAll(new_user_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_sql, query_data)
    return __file__

if __name__ == "__main__":
    # for x in xrange(6, 0, -1):
    # i = 5
    # while i >= 1:
    #     update_hbgj_activeusers_daily(i)
    #     i -= 1
    # update_hbgj_activeusers_daily(1)
    update_hbgj_newuser_daily(1)