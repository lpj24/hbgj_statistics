gtgj_consumers_daily = """
    select date_format(create_time,'%%Y-%%m-%%d') s_day,count(DISTINCT uid) consumers,
    count(distinct case when p_info LIKE '%%ios%%' then uid else null end ) consumers_ios,
    count(distinct case when p_info LIKE '%%android%%' then uid else null end ) consumers_android
    from user_order
    where i_status=3 and create_time>=%s
    and create_time<%s
    group by s_day
"""

update_gtgj_consumers_daily = """
    insert into gtgj_consumers_daily (s_day, consumers, consumers_ios , consumers_android,
    createtime, updatetime) values (%s, %s , %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    consumers = VALUES(consumers),
    consumers_ios = VALUES(consumers_ios),
    consumers_android = VALUES(consumers_android)

"""


gtgj_consumers_weekly = """
    select CONCAT(case
    when DAYOFWEEK(str_to_date(create_time, '%%Y-%%m-%%d'))=1 then DATE_FORMAT((DATE_SUB(str_to_date(create_time, '%%Y-%%m-%%d'),INTERVAL 6 DAY)),'%%Y-%%m-%%d')
    when DAYOFWEEK(str_to_date(create_time, '%%Y-%%m-%%d'))=2 then DATE_FORMAT((DATE_SUB(str_to_date(create_time, '%%Y-%%m-%%d'),INTERVAL 0 DAY)),'%%Y-%%m-%%d')
    when DAYOFWEEK(str_to_date(create_time, '%%Y-%%m-%%d'))=3 then DATE_FORMAT((DATE_SUB(str_to_date(create_time, '%%Y-%%m-%%d'),INTERVAL 1 DAY)),'%%Y-%%m-%%d')
    when DAYOFWEEK(str_to_date(create_time, '%%Y-%%m-%%d'))=4 then DATE_FORMAT((DATE_SUB(str_to_date(create_time, '%%Y-%%m-%%d'),INTERVAL 2 DAY)),'%%Y-%%m-%%d')
    when DAYOFWEEK(str_to_date(create_time, '%%Y-%%m-%%d'))=5 then DATE_FORMAT((DATE_SUB(str_to_date(create_time, '%%Y-%%m-%%d'),INTERVAL 3 DAY)),'%%Y-%%m-%%d')
    when DAYOFWEEK(str_to_date(create_time, '%%Y-%%m-%%d'))=6 then DATE_FORMAT((DATE_SUB(str_to_date(create_time, '%%Y-%%m-%%d'),INTERVAL 4 DAY)),'%%Y-%%m-%%d')
    when DAYOFWEEK(str_to_date(create_time, '%%Y-%%m-%%d'))=7 then DATE_FORMAT((DATE_SUB(str_to_date(create_time, '%%Y-%%m-%%d'),INTERVAL 5 DAY)),'%%Y-%%m-%%d')
    end) s_day,
    count(DISTINCT uid) consumers,
    count(distinct case when p_info LIKE '%%ios%%' then uid else null end ) consumers_ios,
    count(distinct case when p_info LIKE '%%android%%' then uid else null end ) consumers_android
    from (
    select create_time, uid, p_info
    from user_order_history
    where i_status=3 and create_time>=%s
    and create_time<%s
    UNION
    select create_time, uid, p_info
    from user_order
    where i_status=3 and create_time>=%s
    and create_time<%s
    ) as A
    group by s_day

"""


update_gtgj_consumers_weekly = """
    insert into gtgj_consumers_weekly (s_day, consumers, consumers_ios , consumers_android,
    createtime, updatetime) values (%s, %s , %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    consumers = VALUES(consumers),
    consumers_ios = VALUES(consumers_ios),
    consumers_android = VALUES(consumers_android)

"""


gtgj_consumers_monthly = """
    select %s s_day,
    count(DISTINCT uid) consumers,
    count(distinct case when p_info LIKE '%%ios%%' then uid else null end ) consumers_ios,
    count(distinct case when p_info LIKE '%%android%%' then uid else null end ) consumers_android
    from (
    select create_time,uid,p_info
    from user_order_history
    where i_status=3 and create_time>=%s
    and create_time<%s
    UNION
    select create_time,uid,p_info
    from user_order
    where i_status=3 and create_time>=%s
    and create_time<%s
    ) as A
"""

update_gtgj_consumers_monthly = """
    insert into gtgj_consumers_monthly (s_day, consumers, consumers_ios , consumers_android,
    createtime, updatetime) values (%s, %s , %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    consumers = VALUES(consumers),
    consumers_ios = VALUES(consumers_ios),
    consumers_android = VALUES(consumers_android)
"""


gtgj_consumers_quarterly = """
    select CONCAT(YEAR(%s),',','Q',QUARTER(%s)) s_day,
    count(DISTINCT uid) consumers,
    count(distinct case when p_info LIKE '%%ios%%' then uid else null end ) consumers_ios,
    count(distinct case when p_info LIKE '%%android%%' then uid else null end ) consumers_android
    from (
    select create_time,uid,p_info
    from user_order_history
    where i_status=3 and create_time>=%s
    and create_time<%s
    UNION
    select create_time,uid,p_info
    from user_order
    where i_status=3 and create_time>=%s
    and create_time<%s
    ) as A

"""


update_gtgj_consumers_quarterly = """
    insert into gtgj_consumers_quarterly (s_day, consumers, consumers_ios , consumers_android,
    createtime, updatetime) values (%s, %s , %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    consumers = VALUES(consumers),
    consumers_ios = VALUES(consumers_ios),
    consumers_android = VALUES(consumers_android)
"""