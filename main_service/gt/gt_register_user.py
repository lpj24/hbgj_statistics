# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_gtgj_register_user_daily(days=1):
    """更新高铁注册用户数量, gtgj_register_user_daily"""

    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - days), '%Y-%m-%d')
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')

    dto = [start_date, end_date]
    register_sql = """
        select DATE_FORMAT(create_time, '%%Y-%%m-%%d') s_day,
        count(1) register_users,
        sum(case when p_info like '%%ios%%' then 1 else 0 END)   register_users_ios,
        sum(case when p_info like '%%android%%' then 1 else 0 END)   register_users_android
        from account_gtgj
        where create_time>=%s
        and create_time < %s
        GROUP BY s_day
    """

    insert_sql = """
        insert into gtgj_register_user_daily (s_day, register_users, register_users_ios, register_users_android,
        createtime, updatetime) values (%s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        register_users = values(register_users),
        register_users_ios = values(register_users_ios),
        register_users_android = values(register_users_android)
    """

    register_data = DBCli().gt_cli.queryAll(register_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_sql, register_data)


def update_hbgj_register_user_daily(days=1):
    """更新航班注册用户数量, hbgj_register_user_daily"""

    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - days), '%Y-%m-%d')
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')

    dto = [start_date, end_date]
    register_sql = """
        select DATE_FORMAT(createtime, '%%Y-%%m-%%d') s_day,
        count(1) register_users,
        sum(case when p like '%%ios%%' then 1 else 0 END)   register_users_ios,
        sum(case when p like '%%android%%' then 1 else 0 END)   register_users_android
        from phone_user
        where createtime>=%s
        and createtime < %s
        GROUP BY s_day
    """

    insert_sql = """
        insert into hbgj_register_user_daily (s_day, register_users, register_users_ios, register_users_android,
        createtime, updatetime) values (%s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        register_users = values(register_users),
        register_users_ios = values(register_users_ios),
        register_users_android = values(register_users_android)
    """

    register_data = DBCli().sourcedb_cli.queryAll(register_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_sql, register_data)

if __name__ == "__main__":
    # update_gtgj_register_user_daily(1420)
    update_hbgj_register_user_daily(1750)