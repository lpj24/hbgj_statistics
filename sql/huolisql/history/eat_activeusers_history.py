eat_activeusers_daily_history = """
    SELECT %s s_day, count(distinct(user_id))
    FROM log_record
    WHERE log_time_in_millisecond >= %s
    AND log_time_in_millisecond < %s
    AND user_id IS NOT NULL

"""
insert_eat_activeusers_daily_history = """
    insert into huoli_eat_activeusers_daily_test values (%s, %s, now(), now())
"""

eat_activeusers_weekly_history = """
    SELECT %s s_day, count(distinct(user_id))
    FROM log_record
    WHERE log_time_in_millisecond >= %s
    AND log_time_in_millisecond < %s
    AND user_id IS NOT NULL

"""
insert_eat_activeusers_weekly_history = """
    insert into huoli_eat_activeusers_weekly_test values (%s, %s, now(), now())
"""

eat_activeusers_monthly_history = """
    SELECT %s s_day, count(distinct(user_id))
    FROM log_record
    WHERE log_time_in_millisecond >= %s
    AND log_time_in_millisecond < %s
    AND user_id IS NOT NULL

"""
insert_eat_activeusers_monthly_history = """
    insert into huoli_eat_activeusers_monthly_test values (%s, %s, now(), now())
"""

eat_activeusers_quarterly_history = """
    SELECT CONCAT(YEAR(%s),',','Q',QUARTER(%s)) s_day, count(distinct(user_id))
    FROM log_record
    WHERE log_time_in_millisecond >= %s
    AND log_time_in_millisecond < %s
    AND user_id IS NOT NULL

"""
insert_eat_activeusers_quarterly_history = """
    insert into huoli_eat_activeusers_quarterly_test values (%s, %s, now(), now())
"""