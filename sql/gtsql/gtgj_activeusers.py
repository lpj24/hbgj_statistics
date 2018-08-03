gtgj_activeusers_daily = """
    select s_day,active_users,active_users_ios, (active_users-active_users_ios) active_users_android
    from  (SELECT s_day,sum(active_users) active_users,
    sum(case when source LIKE '%%91ZS%%' or source LIKE '%%appstore%%' or source LIKE '%%juwan%%' or source LIKE '%%91PGZS%%'
    or source LIKE '%%kuaiyong%%' or source LIKE '%%TBT%%' or source LIKE '%%PPZS%%' then active_users else 0 end) active_users_ios
    FROM global_statistics
    where s_day<%s and s_day>=%s
    GROUP BY s_day) A

"""

update_gtgj_activeusers_daily = """
    insert into gtgj_activeusers_daily (s_day, active_users, active_users_ios , active_users_android,
    createtime, updatetime) values (%s, %s , %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    active_users = VALUES(active_users),
    active_users_ios = VALUES(active_users_ios),
    active_users_android = VALUES(active_users_android)
"""

gtgj_activeusers_weekly = """
    select s_day,active_users,active_users_ios, (active_users-active_users_ios) active_users_android
    from  (SELECT s_day,sum(active_users) active_users,
    sum(case when source LIKE '%%91ZS%%' or source LIKE '%%appstore%%' or source LIKE '%%juwan%%' or source LIKE '%%91PGZS%%'
    or source LIKE '%%kuaiyong%%' or source LIKE '%%TBT%%' or source LIKE '%%PPZS%%' then active_users else 0 end) active_users_ios
    FROM global_week_statistics
    where s_day>=%s and s_day<%s
    GROUP BY s_day) A
"""

update_gtgj_activeusers_weekly = """
    insert into gtgj_activeusers_weekly (s_day, active_users, active_users_ios , active_users_android,
    createtime, updatetime) values (%s, %s , %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    active_users = VALUES(active_users),
    active_users_ios = VALUES(active_users_ios),
    active_users_android = VALUES(active_users_android)
"""


gtgj_activeusers_monthly = """
    select s_day,active_users,active_users_ios, (active_users-active_users_ios) active_users_android
    from  (SELECT s_day,sum(active_users) active_users,
    sum(case when source LIKE '%%91ZS%%' or source LIKE '%%appstore%%' or source LIKE '%%juwan%%' or source LIKE '%%91PGZS%%'
    or source LIKE '%%kuaiyong%%' or source LIKE '%%TBT%%' or source LIKE '%%PPZS%%' then active_users else 0 end) active_users_ios
    FROM global_month_statistics
    where s_day>=%s and s_day<%s
    GROUP BY s_day) A
"""

update_gtgj_activeusers_monthly = """
    insert into gtgj_activeusers_monthly (s_day, active_users, active_users_ios , active_users_android,
    createtime, updatetime) values (%s, %s , %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    active_users = VALUES(active_users),
    active_users_ios = VALUES(active_users_ios),
    active_users_android = VALUES(active_users_android)
"""

update_gtgj_activeusers_quarterly = """
    insert into hbgj_activeusers_quarterly (s_day, active_users, active_users_ios , active_users_android,
    createtime, updatetime) values (%s, %s , %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    active_users = VALUES(active_users),
    active_users_ios = VALUES(active_users_ios),
    active_users_android = VALUES(active_users_android)
"""


gtgj_newusers_daily = """
    select s_day,new_users,new_users_ios, (new_users-new_users_ios) new_users_android from (
    SELECT s_day,sum(new_users) new_users,
    sum(case when source LIKE '%%91ZS%%' or source LIKE '%%appstore%%' or source LIKE '%%juwan%%'
    or source LIKE '%%91PGZS%%' or source LIKE '%%kuaiyong%%' or source LIKE '%%TBT%%' or source LIKE '%%PPZS%%' then new_users else 0 end) new_users_ios
    FROM global_statistics
    where s_day<%s and s_day>=%s
    GROUP BY s_day) A
"""

update_gtgj_newusers_daily = """
    insert into gtgj_newusers_daily (s_day, new_users, new_users_ios , new_users_android,
    createtime, updatetime) values (%s, %s , %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    new_users = VALUES(new_users),
    new_users_ios = VALUES(new_users_ios),
    new_users_android = VALUES(new_users_android)
"""

