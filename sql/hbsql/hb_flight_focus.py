# hb_flight_focus_users_daily = """
#     select %s,sum(AA.focus_users) from (
#     select count(DISTINCT A.phoneid) focus_users from (
#         SELECT phoneid FROM FLY_USERFOCUS_TBL
#         where PHONEID>0
#         and FOCUSTIME<%s
#         and FOCUSTIME>=%s
#         union
#         SELECT phoneid FROM FLY_USERFOCUS_TBL_HIS
#         where PHONEID>0
#         and FOCUSTIME<%s
#         and FOCUSTIME>=%s
#     ) A
#     UNION
#     select count(DISTINCT B.userid) focus_users from (
#         SELECT userid from FLY_USERFOCUS_TBL
#         where PHONEID=0
#         and FOCUSTIME<%s
#         and FOCUSTIME>=%s
#         UNION
#         SELECT USERID from FLY_USERFOCUS_TBL_HIS
#         where PHONEID=0
#         and FOCUSTIME<%s
#         and FOCUSTIME>=%s
#     ) B) AA
# """

hb_flight_focus_users_daily = """
    SELECT %s s_day,
    count(DISTINCT case when phoneid > 0 then phoneid else userid end) focus_num
    FROM FLY_USERFOCUS_TBL
    where  FOCUSTIME<%s
    and FOCUSTIME>=%s
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