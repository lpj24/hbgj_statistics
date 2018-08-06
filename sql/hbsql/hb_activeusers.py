hbgj_activeusers_daily = """
    select A.s_day,A.active_users, A.active_users_ios,
    (A.active_users-A.active_users_ios) active_users_android
    from (select DATE_FORMAT(createtime,'%%Y-%%m-%%d') s_day,count(distinct userid) active_users,
    sum(case when p LIKE '%%91ZS%%' or p LIKE '%%appstore%%' or p LIKE '%%juwan%%' or p LIKE '%%91PGZS%%'
    or p LIKE '%%kuaiyong%%' or p LIKE '%%TBT%%' or p LIKE '%%PPZS%%' then 1 else 0 end ) active_users_ios
    from ACTIVE_USER_LOG
    where DATE_FORMAT(createtime,'%%Y-%%m-%%d')>=%s
    and DATE_FORMAT(createtime,'%%Y-%%m-%%d')<%s
    and p like '%%hbgj%%'
    GROUP BY
) A;
"""

update_hbgj_activeusers_daily = """
    insert into hbgj_activeusers_daily (s_day, active_users, active_users_ios , active_users_android,
    createtime, updatetime) values (%s, %s , %s, %s, now(), now())
    on duplicate key update updatetime = now(),
    active_users = VALUES(active_users),
    active_users_ios = VALUES(active_users_ios),
    active_users_android = VALUES(active_users_android)
"""

hbgj_activeusers_weekly = """
    select A.s_day,A.active_users, A.active_users_ios,
    (A.active_users-A.active_users_ios) active_users_android
    from (select DATE_FORMAT(subdate(createtime, weekday(createtime)), '%%Y-%%m-%%d') s_day,
count(distinct userid) active_users,
    count(distinct case when p LIKE '%%91ZS%%' or p LIKE '%%appstore%%' or p LIKE '%%juwan%%' or p LIKE '%%91PGZS%%'
    or p LIKE '%%kuaiyong%%' or p LIKE '%%TBT%%' or p LIKE '%%PPZS%%' then USERID else null end ) active_users_ios
    from ACTIVE_USER_LOG
    where DATE_FORMAT(createtime,'%%Y-%%m-%%d')>=%s
    and DATE_FORMAT(createtime,'%%Y-%%m-%%d')<%s
    and p like '%%hbgj%%'
GROUP BY s_day
) A;
"""

update_hbgj_activeusers_weekly = """
    insert into hbgj_activeusers_weekly (s_day, active_users, active_users_ios , active_users_android,
    createtime, updatetime) values (%s, %s, %s, %s, now(), now())
    on duplicate key update updatetime = now(),
    active_users = VALUES(active_users),
    active_users_ios = VALUES(active_users_ios),
    active_users_android = VALUES(active_users_android)
"""

hbgj_activeusers_monthly = """
    select A.s_day,A.active_users, A.active_users_ios,
    (A.active_users-A.active_users_ios) active_users_android
    from (select DATE_FORMAT(subdate(createtime, day(createtime) - 1), '%%Y-%%m-%%d') s_day,
count(distinct userid) active_users,
    count(distinct case when p LIKE '%%91ZS%%' or p LIKE '%%appstore%%' or p LIKE '%%juwan%%' or p LIKE '%%91PGZS%%'
    or p LIKE '%%kuaiyong%%' or p LIKE '%%TBT%%' or p LIKE '%%PPZS%%' then USERID else null end ) active_users_ios
    from ACTIVE_USER_LOG
    where DATE_FORMAT(createtime,'%%Y-%%m-%%d')>=%s
    and DATE_FORMAT(createtime,'%%Y-%%m-%%d')<%s
    and p like '%%hbgj%%'
GROUP BY s_day
) A;
"""

update_hbgj_activeusers_monthly = """
    insert into hbgj_activeusers_monthly (s_day, active_users, active_users_ios , active_users_android,
    createtime, updatetime) values (%s, %s , %s, %s, now(), now())
    on duplicate key update updatetime = now(),
    active_users = VALUES(active_users),
    active_users_ios = VALUES(active_users_ios),
    active_users_android = VALUES(active_users_android)
"""
