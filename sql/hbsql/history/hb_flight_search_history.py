hb_filght_search_user_daily_history = """
    SELECT count(1),%s s_day FROM tablename where logTime>=%s
    and logTime<%s
    and (pid = '4312' or pid = '4313')
"""

update_flight_search_user_daily_history = """
    insert into hbdt_search_daily (s_day, search_users,
    createtime, updatetime) values (%s, %s, now(), now())
"""


hb_filght_search_user_weekly_history = """
    SELECT count(1), %s s_day FROM tablename where logTime>=%s
    and logTime<%s
    and (pid = '4312' or pid = '4313')
"""

hb_filght_search_user_difftable_weekly = """
        select count(1), %s s_day
        from (
        select uid, logTime FROM tablename
        where logTime>= %s and logTime<%s
        and  (pid = '4312' or pid = '4313')
        union all
        select uid, logTime FROM tablename
        where logTime>= %s and logTime<%s
        and  (pid = '4312' or pid = '4313')) A

"""

update_flight_search_user_weekly_history = """
    insert into hbdt_search_weekly (s_day, search_users,
    createtime, updatetime) values (%s, %s, now(), now())
"""


hb_filght_search_user_monthly_history = """
    select count(1), %s s_day from (
    select uid FROM tablename
    where (pid = '4312' or pid = '4313')
    union
    select uid from tablename
    where  (pid = '4312' or pid = '4313')
    union
    select uid from tablename
    where (pid = '4312' or pid = '4313')) as tmp_tab
"""

update_flight_search_user_monthly_history = """
    insert into hbdt_search_monthly (s_day, search_users,
    createtime, updatetime) values (%s, %s, now(), now())
"""

hb_filght_search_user_quarterly_history = """
        select  count(1), CONCAT(YEAR(%s),',','Q',QUARTER(%s)) s_day  from (
        select uid FROM tablename
        where (pid = '4312' or pid = '4313')
        union
        select uid from tablename
        where  (pid = '4312' or pid = '4313')
        union
        select uid from tablename
        where  (pid = '4312' or pid = '4313')
        UNION
        select uid FROM tablename
        where  (pid = '4312' or pid = '4313')
        union
        select uid from tablename
        where  (pid = '4312' or pid = '4313')
        union
        select uid from tablename
        where  (pid = '4312' or pid = '4313')
        UNION
        select uid FROM tablename
        where  (pid = '4312' or pid = '4313')
        union
        select uid from tablename
        where  (pid = '4312' or pid = '4313')
        union
        select uid from tablename
        where  (pid = '4312' or pid = '4313')) as tmp_tab;
"""

update_flight_search_user_quarterly_history = """
    insert into hbdt_search_quarterly (s_day, search_users,
    createtime, updatetime) values (%s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    search_users = VALUES(search_users)
"""

update_flight_search_user_pv_daily_history = """
      update hbdt_search_daily set pv=%s where s_day=%s
"""

update_flight_search_user_pv_weekly_history = """
      update hbdt_search_weekly set pv=%s where s_day=%s
"""

update_flight_search_user_pv_monthly_history = """
      update hbdt_search_monthly set pv=%s where s_day=%s
"""

update_flight_search_user_pv_quarterly_history = """
      update hbdt_search_quarterly set pv=%s where s_day=%s
"""