hb_filght_detail_user_daily_history = """
    SELECT %s s_day
    , count(distinct uid) FROM tablename where logTime>=%s
    and logTime<%s
    and (pid = '4312' or pid = '4313')
"""

update_flight_detail_user_daily_history = """
    insert into hbdt_details_daily (s_day, detail_users,
    createtime, updatetime) values (%s, %s, now(), now())
"""


hb_filght_detail_user_weekly_history = """
    SELECT count(1), %s s_day FROM tablename where logTime>=%s
    and logTime<%s
    and pid='4314'
"""

hb_filght_detail_user_difftable_weekly = """
        select count(1), %s s_day
        from (
        select uid, logTime FROM tablename
        where logTime>= %s and logTime<%s
        and pid='4314'
        union all
        select uid, logTime FROM tablename
        where logTime>= %s and logTime<%s
        and pid='4314') A

"""

update_flight_detail_user_weekly_history = """
    insert into hbdt_details_weekly (s_day, detail_users,
    createtime, updatetime) values (%s, %s, now(), now())
"""


hb_filght_detail_user_monthly_history = """
    select count(1), %s s_day from (
    select uid FROM tablename
    where pid='4314'
    union
    select uid from tablename
    where pid='4314'
    union
    select uid from tablename
    where pid='4314') as tmp_tab
"""

update_flight_detail_user_monthly_history = """
    insert into hbdt_details_monthly (s_day, detail_users,
    createtime, updatetime) values (%s, %s, now(), now())
"""

hb_filght_detail_user_quarterly_history = """
        select  count(1), CONCAT(YEAR(%s),',','Q',QUARTER(%s)) s_day from (
        select uid FROM tablename
        where pid='4314'
        union
        select uid from tablename
        where pid='4314'
        union
        select uid from tablename
        where pid='4314'
        UNION
        select uid FROM tablename
        where pid='4314'
        union
        select uid from tablename
        where pid='4314'
        union
        select uid from tablename
        where pid='4314'
        UNION
        select uid FROM tablename
        where pid='4314'
        union
        select uid from tablename
        where pid='4314'
        union
        select uid from tablename
        where pid='4314') as tmp_tab;
"""

update_flight_detail_user_quarterly_history = """
    insert into hbdt_details_quarterly (s_day, detail_users,
    createtime, updatetime) values (%s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    detail_users = VALUES(detail_users)
"""

flight_detail_user_query_daily_his = """
    SELECT count(1),
    date_format(logTime, '%%Y-%%m-%%d') s_day FROM tablename where logTime>=%s
    and logTime<%s
    and pid='4314'
"""

update_flight_detail_user_query_daily_his = """
    update hbdt_details_daily set query_count=%s where s_day=%s
"""

update_flight_detail_user_query_weekly_his = """
    update hbdt_details_weekly set pv=%s where s_day=%s
"""

update_flight_detail_user_query_monthly_his = """
    update hbdt_details_monthly set pv=%s where s_day=%s
"""

update_flight_detail_user_query_quarterly_his = """
    update hbdt_details_quarterly set pv=%s where s_day=%s
"""