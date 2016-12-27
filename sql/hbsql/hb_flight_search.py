hb_filght_search_user_daily = """
    SELECT %s s_day
    , count(distinct uid), count(1) FROM tablename where logTime>=%s
    and logTime<%s
    and (pid = '4312' or pid = '4313')
"""

update_flight_search_user_daily = """
    insert into hbdt_search_daily (s_day, search_users, pv, check_pv, localytics_uv, localytics_pv,
    createtime, updatetime) values (%s, %s, %s, %s, %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    search_users = VALUES(search_users),
    pv = VALUES(pv),
    check_pv = VALUES(check_pv),
    localytics_uv = VALUES(localytics_uv),
    localytics_pv = VALUES(localytics_pv)
"""


hb_filght_search_user_weekly = """
    SELECT date_format(%s, '%%Y-%%m-%%d') s_day,count(distinct uid), count(1)
    FROM tablename
    where logTime<%s
    and logTime>= %s
    and  (pid = '4312' or pid = '4313')

"""

update_flight_search_user_weekly = """
    insert into hbdt_search_weekly (s_day, search_users, pv,
    createtime, updatetime) values (%s, %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    search_users = VALUES(search_users),
    pv = VALUES(pv)
"""

hb_filght_search_user_table_weekly = """
        select date_format(%s, '%%Y-%%m-%%d') s_day ,count(distinct A.uid), count(1)
        from (
        select uid, logTime FROM tablename
        where logTime<%s
        and logTime>= %s
        and  (pid = '4312' or pid = '4313')
        union all
        select uid, logTime FROM tablename
        where logTime<%s
        and logTime>=%s
        and (pid = '4312' or pid = '4313')) A

"""


hb_filght_search_user_monthly = """
    select date_format(%s, '%%Y-%%m-%%d') s_day,count(distinct tmp_tab.uid), count(1) from (
    select uid FROM tablename
    where (pid = '4312' or pid = '4313')
    union
    select uid from tablename
    where (pid = '4312' or pid = '4313')
    union
    select uid from tablename
    where (pid = '4312' or pid = '4313')) as tmp_tab
"""

update_flight_search_user_monthly = """
    insert into hbdt_search_monthly (s_day, search_users, pv,
    createtime, updatetime) values (%s, %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    search_users = VALUES(search_users),
    pv = VALUES(pv)
"""

hb_filght_search_user_quarterly = """
        select CONCAT(YEAR(%s),',','Q',QUARTER(%s)) s_day, count(distinct tmp_tab.uid),count(1)  from (
        select uid FROM tablename
        where (pid = '4312' or pid = '4313')
        union
        select uid from tablename
        where (pid = '4312' or pid = '4313')
        union
        select uid from tablename
        where (pid = '4312' or pid = '4313')
        UNION
        select uid FROM tablename
        where (pid = '4312' or pid = '4313')
        union
        select uid from tablename
        where (pid = '4312' or pid = '4313')
        union
        select uid from tablename
        where (pid = '4312' or pid = '4313')
        UNION
        select uid FROM tablename
        where (pid = '4312' or pid = '4313')
        union
        select uid from tablename
        where (pid = '4312' or pid = '4313')
        union
        select uid from tablename
        where (pid = '4312' or pid = '4313')) as tmp_tab;
"""

update_flight_search_user_quarterly = """
    insert into hbdt_search_quarterly (s_day, search_users, pv,
    createtime, updatetime) values (%s, %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    search_users = VALUES(search_users),
    pv = VALUES(pv)
"""