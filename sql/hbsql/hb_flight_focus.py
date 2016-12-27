hb_flight_focus_users_daily = """
    select :s_day s_day,sum(focus_users) from (
    select count(DISTINCT phoneid) focus_users from (
        SELECT phoneid FROM FLY_USERFOCUS_TBL
        where PHONEID>0
        and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
        and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
        union
        SELECT phoneid focus_users FROM FLY_USERFOCUS_TBL_HIS
        where PHONEID>0
        and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
        and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
    )
    UNION
    select count(DISTINCT userid) focus_users from (
        SELECT userid from FLY_USERFOCUS_TBL
        where PHONEID=0
        and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
        and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
        UNION
        SELECT USERID from FLY_USERFOCUS_TBL_HIS
        where PHONEID=0
        and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
        and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
    ) )
"""

update_flight_focus_user_daily = """
    insert into hbdt_focus_daily (s_day, focus_users, focus_pv,
    createtime, updatetime) values (%s, %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    focus_users = VALUES(focus_users),
    focus_pv = VALUES(focus_pv)
"""


hb_flight_focus_users_weekly = """
select :s_day s_day,sum(focus_users) from (
select count(DISTINCT phoneid) focus_users from (
    SELECT phoneid FROM FLY_USERFOCUS_TBL
    where PHONEID>0
    and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
    and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
    union
    SELECT phoneid focus_users FROM FLY_USERFOCUS_TBL_HIS
    where PHONEID>0
    and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
    and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
)
UNION
select count(DISTINCT userid) focus_users from (
    SELECT userid from FLY_USERFOCUS_TBL
    where PHONEID=0
    and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
    and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
    UNION
    SELECT USERID from FLY_USERFOCUS_TBL_HIS
    where PHONEID=0
    and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
    and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
) )
"""

update_flight_focus_user_weekly = """
    insert into hbdt_focus_weekly (s_day, focus_users,
    createtime, updatetime) values (%s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    focus_users = VALUES(focus_users)
"""

hb_flight_focus_users_monthly = """
select :s_day s_day,sum(focus_users) from (
select count(DISTINCT phoneid) focus_users from (
    SELECT phoneid FROM FLY_USERFOCUS_TBL
    where PHONEID>0
    and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
    and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
    union
    SELECT phoneid focus_users FROM FLY_USERFOCUS_TBL_HIS
    where PHONEID>0
    and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
    and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
)
UNION
select count(DISTINCT userid) focus_users from (
    SELECT userid from FLY_USERFOCUS_TBL
    where PHONEID=0
    and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
    and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
    UNION
    SELECT USERID from FLY_USERFOCUS_TBL_HIS
    where PHONEID=0
    and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
    and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
) )
"""

update_flight_focus_user_monthly = """
    insert into hbdt_focus_monthly (s_day, focus_users,
    createtime, updatetime) values (%s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    focus_users = VALUES(focus_users)
"""

hb_flight_focus_users_quarterly = """
   select to_char(to_date(:s_day, 'YYYY-MM-DD'), 'yyyy') || ',Q' || to_char(to_date(:s_day, 'YYYY-MM-DD'), 'q') s_day,
   sum(focus_users) from (
    select count(DISTINCT phoneid) focus_users from (
    SELECT phoneid FROM FLY_USERFOCUS_TBL
    where PHONEID>0
    and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
    and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
    union
    SELECT phoneid focus_users FROM FLY_USERFOCUS_TBL_HIS
    where PHONEID>0
    and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
    and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
    )
    UNION
    select count(DISTINCT userid) focus_users from (
        SELECT userid from FLY_USERFOCUS_TBL
        where PHONEID=0
        and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
        and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
        UNION
        SELECT USERID from FLY_USERFOCUS_TBL_HIS
        where PHONEID=0
        and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
        and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
    ) )
"""

update_flight_focus_user_quarterly = """
    insert into hbdt_focus_quarterly (s_day, focus_users,
    createtime, updatetime) values (%s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    focus_users = VALUES(focus_users)
"""