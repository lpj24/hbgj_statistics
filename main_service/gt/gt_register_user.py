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
        count(1) all_users,
        sum(case when p_info like '%%ios%%' and (gt_user_name like 'W%%' or gt_user_name like 'H%%') then 1 else 0 END) ios_weixin_users,
        sum(case when p_info like '%%android%%' and (gt_user_name like 'W%%' or gt_user_name like 'H%%')  then 1 else 0 END) android_weixin_users,
        sum(case when p_info like '%%ios%%' and gt_user_name not like 'W%%' and gt_user_name not like 'H%%' then 1 else 0 END) ios_phone_users,
        sum(case when p_info like '%%android%%' and gt_user_name not like 'W%%' and gt_user_name not like 'H%%' then 1 else 0 END) android_phone_users,
        sum(case when p_info='p' then 1 else 0 end) weixin_users,
        sum(case when p_info not like '%%ios%%' and p_info not like '%%android%%' and p_info!= 'p' then 1 else 0 end) else_users
        from account_gtgj
        where create_time>=%s
        and create_time<%s
        group by s_day
    """

    insert_sql = """
        insert into gtgj_register_user_daily (s_day, all_users, ios_weixin_users, android_weixin_users,
        ios_phone_users, android_phone_users, weixin_users, else_users,
        createtime, updatetime) values (%s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        all_users = values(all_users),
        ios_weixin_users = values(ios_weixin_users),
        android_weixin_users = values(android_weixin_users),
        ios_phone_users = values(ios_phone_users),
        android_phone_users = values(android_phone_users),
        weixin_users = values(weixin_users),
        else_users = values(else_users)
    """

    register_data = DBCli().gt_cli.query_all(register_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, register_data)


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

    register_data = DBCli().sourcedb_cli.query_all(register_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, register_data)

if __name__ == "__main__":
    update_gtgj_register_user_daily(1421)
    # update_hbgj_register_user_daily(1750)