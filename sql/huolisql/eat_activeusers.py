eat_activeusers_daily = """
    SELECT %s s_day, count(distinct(user_id)) active_users
    FROM log_record
    WHERE log_time_in_millisecond >= %s
    AND log_time_in_millisecond < %s
    AND user_id IS NOT NULL

"""
insert_eat_activeusers_daily = """
    insert into huoli_eat_activeusers_daily_test values (%s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    active_users = VALUES(active_users)
"""



eat_activeusers_weekly = """
    SELECT %s s_day, count(distinct(user_id)) active_users
    FROM log_record
    WHERE log_time_in_millisecond >= %s
    AND log_time_in_millisecond < %s
    AND user_id IS NOT NULL

"""
insert_eat_activeusers_weekly = """
    insert into huoli_eat_activeusers_weekly_test values (%s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    active_users = VALUES(active_users)
"""


eat_activeusers_monthly = """
    SELECT %s s_day, count(distinct(user_id)) active_users
    FROM log_record
    WHERE log_time_in_millisecond >= %s
    AND log_time_in_millisecond < %s
    AND user_id IS NOT NULL

"""
insert_eat_activeusers_monthly = """
    insert into huoli_eat_activeusers_monthly_test values (%s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    active_users = VALUES(active_users)
"""

eat_activeusers_quarterly = """
    SELECT CONCAT(YEAR(%s),',','Q',QUARTER(%s)) s_day, count(distinct(user_id)) active_users
    FROM log_record
    WHERE log_time_in_millisecond >= %s
    AND log_time_in_millisecond < %s
    AND user_id IS NOT NULL

"""
insert_eat_activeusers_quarterly = """
    insert into huoli_eat_activeusers_quarterly_test values (%s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    active_users = VALUES(active_users)
"""
