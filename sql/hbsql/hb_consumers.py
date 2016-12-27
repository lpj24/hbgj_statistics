hb_consumers_daily = """
select A.s_day, A.consumers, A.consumers_ios, (consumers-consumers_ios) consumers_android from
(select DATE_FORMAT(createtime,'%%Y-%%m-%%d') s_day,
      count(DISTINCT PHONEID) consumers,
      count(distinct case when p LIKE '%%ios%%' then PHONEID else null end ) consumers_ios
      from(
      SELECT createtime,phoneid,p
      from gift_order
      where PRODUCTID=12
      and createtime >= %s
      and createtime < %s
      UNION
      SELECT createtime,phoneid,p
      from TICKET_ORDER
      where ORDERSTATUE not in (2,12,21,51,75)
      and createtime >= %s
      and createtime < %s
      ) as A
      GROUP BY s_day
      order BY s_day) A
"""

update_hb_consumers_daily = """
    insert into hbgj_consumers_daily (s_day, consumers, consumers_ios , consumers_android,
    createtime, updatetime) values (%s, %s , %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    consumers = VALUES(consumers),
    consumers_ios = VALUES(consumers_ios),
    consumers_android = VALUES(consumers_android)
"""

hb_consumers_weekly = """
select A.s_day, A.consumers, A.consumers_ios, (consumers-consumers_ios) consumers_android from
(
select date_format(subdate(createtime,date_format(createtime,'%%w')-1),'%%Y-%%m-%%d') s_day,
              count(DISTINCT PHONEID) consumers,
              count(distinct case when p LIKE '%%ios%%' then PHONEID else null end ) consumers_ios
              from(
              SELECT createtime,phoneid,p
              from gift_order
              where PRODUCTID=12
              and createtime >= %s
              and createtime < %s
              UNION
              SELECT createtime,phoneid,p
              from TICKET_ORDER
              where ORDERSTATUE not in (2,12,21,51,75)
              and createtime >= %s
              and createtime < %s
              ) as A
              GROUP BY s_day
              order BY s_day) A

"""

update_hb_consumers_weekly = """
    insert into hbgj_consumers_weekly_test (s_day, consumers, consumers_ios , consumers_android,
    createtime, updatetime) values (%s, %s , %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    consumers = VALUES(consumers),
    consumers_ios = VALUES(consumers_ios),
    consumers_android = VALUES(consumers_android)

"""

hb_consumers_monthly = """
select A.s_day, A.consumers, A.consumers_ios, (consumers-consumers_ios) consumers_android from
(select DATE_FORMAT(str_to_date(CONCAT(YEAR(createtime),'-',MONTH(createtime),'-01'),'%%Y-%%m-%%d'),'%%Y-%%m-%%d') s_day,
      count(DISTINCT PHONEID) consumers,
      count(distinct case when p LIKE '%%ios%%' then PHONEID else null end ) consumers_ios
      from(
      SELECT createtime,phoneid,p
      from gift_order
      where PRODUCTID=12
      and createtime >= %s
      and createtime < %s
      UNION
      SELECT createtime,phoneid,p
      from TICKET_ORDER
      where ORDERSTATUE not in (2,12,21,51,75)
      and createtime >= %s
      and createtime < %s
      ) as A
      GROUP BY s_day
      order BY s_day) A

"""

update_hb_consumers_monthly = """
    insert into hbgj_consumers_monthly_test (s_day, consumers, consumers_ios , consumers_android,
    createtime, updatetime) values (%s, %s , %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    consumers = VALUES(consumers),
    consumers_ios = VALUES(consumers_ios),
    consumers_android = VALUES(consumers_android)

"""

hb_consumers_quarterly = """
select A.s_day, A.consumers, A.consumers_ios, (consumers-consumers_ios) consumers_android from
(
select CONCAT(YEAR(createtime),',','Q',QUARTER(createtime)) s_day,
          count(DISTINCT PHONEID) consumers,
          count(distinct case when p LIKE '%%ios%%' then PHONEID else null end ) consumers_ios
          from(
          SELECT createtime,phoneid,p
          from gift_order
          where PRODUCTID=12
          and createtime >= %s
          and createtime < %s
          UNION
          SELECT createtime,phoneid,p
          from TICKET_ORDER
          where ORDERSTATUE not in (2,12,21,51,75)
          and createtime >= %s
          and createtime < %s
          ) as A
          GROUP BY s_day
          order BY s_day) A

"""

update_hb_consumers_quarterly = """
    insert into hbgj_consumers_quarterly (s_day, consumers, consumers_ios , consumers_android,
    createtime, updatetime) values (%s, %s , %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    consumers = VALUES(consumers),
    consumers_ios = VALUES(consumers_ios),
    consumers_android = VALUES(consumers_android)

"""

hb_newconsumers_daily = """
select A.s_day, A.new_consumers, A.new_consumers_ios, (new_consumers-new_consumers_ios) new_consumers_android from
(SELECT date_format(createtime,'%%Y-%%m-%%d') s_day, count(DISTINCT PHONEID) new_consumers,
        count(distinct case when p LIKE '%%ios%%' then PHONEID else null end ) new_consumers_ios
        from (
        SELECT createtime,phoneid,p
        from gift_order
        where PRODUCTID=12
        and DATE_FORMAT(createtime,'%%Y%%m%%d')= %s
        UNION
        SELECT createtime,phoneid,p
        from TICKET_ORDER
        where ORDERSTATUE not in (2,12,21,51,75)
        and DATE_FORMAT(createtime,'%%Y%%m%%d')= %s
        ) as A
        where PHONEID not in
        (
        SELECT phoneid
        from gift_order
        where PRODUCTID=12
        and DATE_FORMAT(createtime,'%%Y%%m%%d')< %s
        UNION
        SELECT phoneid
        from TICKET_ORDER
        where ORDERSTATUE not in (2,12,21,51,75)
        and DATE_FORMAT(createtime,'%%Y%%m%%d')< %s
        ) group by s_day) A

"""

update_hb_newconsumers_daily = """
    insert into hbgj_newconsumers_daily (s_day, new_consumers, new_consumers_ios , new_consumers_android,
    createtime, updatetime) values (%s, %s , %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    new_consumers = VALUES(new_consumers),
    new_consumers_ios = VALUES(new_consumers_ios),
    new_consumers_android = VALUES(new_consumers_android)

"""