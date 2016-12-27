gtgj_order_daily = """
    select
    AA.s_day s_day, AA.ticket_num ticket_num, AA.order_num order_num, AA.gmv gmv,
    BB.ticket_num ticket_num_ios,BB.order_num order_num_ios, BB.gmv gmv_ios,
    CC.ticket_num ticket_num_android,CC.order_num order_num_android, CC.gmv gmv_android,
    DD.ticket_num ticket_num_create,
    DD.order_num order_num_create,
    EE.ticket_num ticket_num_create_ios,
    FF.ticket_num ticket_num_create_android,
    EE.order_num order_num__create_ios,
    FF.order_num order_num_create_android,
    DD.gmv gmv_create, EE.gmv gmv_create_ios, FF.gmv gmv_create_android
    from (select s_day,count(*) order_num,sum(ticket_count) ticket_num, sum(amount) gmv
        from (
        select DATE_FORMAT(create_time,'%%Y-%%m-%%d') s_day,
        uid,depart_date,train_no,ticket_count,amount
        from user_order
        where create_time>=%s
        and create_time<%s
        and i_status=3
        and p_info like '%%ios%%'
        GROUP BY s_day,uid,depart_date,train_no
        ) as B
       GROUP BY s_day ) as BB
LEFT JOIN (select s_day,count(*) order_num,sum(ticket_count) ticket_num, sum(amount) gmv
        from (
        select DATE_FORMAT(create_time,'%%Y-%%m-%%d') s_day,
        uid,depart_date,train_no,ticket_count,amount
        from user_order
        where create_time>=%s
        and create_time<%s
        and i_status=3
        GROUP BY s_day,uid,depart_date,train_no
        ) as A
       GROUP BY s_day ) AA on BB.s_day = AA.s_day
LEFT JOIN (select s_day,count(*) order_num,sum(ticket_count) ticket_num, sum(amount) gmv
        from (
        select DATE_FORMAT(create_time,'%%Y-%%m-%%d') s_day,
        uid,depart_date,train_no,ticket_count,amount
        from user_order
        where create_time>=%s
        and create_time<%s
        and i_status=3
        and p_info like '%%android%%'
        GROUP BY s_day,uid,depart_date,train_no
        ) as A
       GROUP BY s_day ) CC on AA.s_day = CC.s_day
LEFT JOIN (select s_day,count(*) order_num,sum(ticket_count) ticket_num, sum(amount) gmv
        from (
        select DATE_FORMAT(create_time,'%%Y-%%m-%%d') s_day,
        uid,depart_date,train_no,ticket_count,amount
        from user_order
        where create_time>=%s
        and create_time<%s
        and i_status=1
        GROUP BY s_day,uid,depart_date,train_no
        ) as A
       GROUP BY s_day ) DD on CC.s_day = DD.s_day
LEFT JOIN (select s_day,count(*) order_num,sum(ticket_count) ticket_num, sum(amount) gmv
        from (
        select DATE_FORMAT(create_time,'%%Y-%%m-%%d') s_day,
        uid,depart_date,train_no,ticket_count,amount
        from user_order
        where create_time>=%s
        and create_time<%s
        and i_status=1
        and p_info like '%%ios%%'
        GROUP BY s_day,uid,depart_date,train_no
        ) as A
       GROUP BY s_day ) EE on DD.s_day = EE.s_day
LEFT JOIN (select s_day,count(*) order_num,sum(ticket_count) ticket_num, sum(amount) gmv
        from (
        select DATE_FORMAT(create_time,'%%Y-%%m-%%d') s_day,
        uid,depart_date,train_no,ticket_count,amount
        from user_order
        where create_time>=%s
        and create_time<%s
        and i_status=1
        and p_info like '%%android%%'
        GROUP BY s_day,uid,depart_date,train_no
        ) as A
       GROUP BY s_day ) FF on EE.s_day = FF.s_day
"""

gtgj_order_daily_his = """
    select AA.s_day, AA.ticket_num, AA.order_num, AA.amount_num,
    AA.ticket_num_ios, AA.order_num_ios, AA.amount_num_ios,
    (AA.ticket_num-AA.ticket_num_ios) ticket_num_android, (AA.order_num-AA.order_num_ios) order_num_android,
    (AA.amount_num-AA.amount_num_ios) amount_num_android,
    BB.ticket_num ticket_num_create,
    BB.order_num order_num_create ,
    CC.ticket_num ticket_num_create_ios,
    DD.ticket_num ticket_num_create_android,
    CC.order_num order_num_create_ios ,
    DD.order_num order_num_create_android ,
    BB.gmv gmv_create,  CC.gmv gmv_create_ios, DD.gmv gmv_create_android
    from (
    select s_day,
    sum(succ_orders) order_num, sum(succ_tickets) ticket_num, sum(succ_amount) amount_num,
    sum(case when source in ('91PGZS', '91ZS', 'appstore', 'ent299', 'juwan', 'kuaiyong', 'appstorepro',
    'PPZS', 'TBT', 'appstore32') then succ_orders end) order_num_ios,
    sum(case when source in ('91PGZS', '91ZS', 'appstore', 'ent299', 'juwan', 'kuaiyong', 'appstorepro',
    'PPZS', 'TBT', 'appstore32') then succ_tickets end) ticket_num_ios,
    sum(case when source in ('91PGZS', '91ZS', 'appstore', 'ent299', 'juwan', 'kuaiyong', 'appstorepro',
    'PPZS', 'TBT', 'appstore32') then succ_amount end) amount_num_ios
    from global_statistics
    where s_day>=date_format(%s, '%%Y-%%m-%%d')
    and s_day<date_format(%s, '%%Y-%%m-%%d')
    group by s_day) as AA
LEFT JOIN (select s_day,count(*) order_num,sum(ticket_count) ticket_num, sum(amount) gmv
    from (
    select DATE_FORMAT(create_time,'%%Y-%%m-%%d') s_day,
    uid,depart_date,train_no,ticket_count,amount
    from user_order
    where create_time>=%s
    and create_time<%s
    and i_status=1
    GROUP BY s_day,uid,depart_date,train_no
    ) as A
   GROUP BY s_day) as BB on AA.s_day = BB.s_day
LEFT JOIN (select s_day,count(*) order_num,sum(ticket_count) ticket_num, sum(amount) gmv
    from (
    select DATE_FORMAT(create_time,'%%Y-%%m-%%d') s_day,
    uid,depart_date,train_no,ticket_count,amount
    from user_order
    where create_time>=%s
    and create_time<%s
    and i_status=1
    and p_info like '%%ios%%'
    GROUP BY s_day,uid,depart_date,train_no
    ) as A
   GROUP BY s_day) CC on BB.s_day = CC.s_day
LEFT JOIN (select s_day,count(*) order_num,sum(ticket_count) ticket_num, sum(amount) gmv
    from (
    select DATE_FORMAT(create_time,'%%Y-%%m-%%d') s_day,
    uid,depart_date,train_no,ticket_count,amount
    from user_order
    where create_time>=%s
    and create_time<%s
    and i_status=1
    and p_info like '%%android%%'
    GROUP BY s_day,uid,depart_date,train_no
    ) as A
   GROUP BY s_day) DD on CC.s_day = DD.s_day

"""

update_gtgj_order_daily = """
    insert into gtgj_order_daily
    (s_day,ticket_num,order_num,gmv,
    ticket_num_ios,order_num_ios,gmv_ios,
    ticket_num_android,order_num_android,gmv_android,
    ticket_num_create,order_num_create,ticket_num_create_ios,ticket_num_create_android,
    order_num_create_ios,order_num_create_android,
    gmv_create,gmv_create_ios,gmv_create_android,
    createtime, updatetime
    ) values (%s, %s , %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s, now(), now())
    on duplicate key update updatetime = now() ,
    ticket_num = VALUES(ticket_num),
    order_num = VALUES(order_num),
    gmv = VALUES(gmv),
    ticket_num_ios = VALUES(ticket_num_ios),
    order_num_ios = VALUES(order_num_ios),
    gmv_ios = VALUES(gmv_ios),
    ticket_num_android = VALUES(ticket_num_android),
    order_num_android = VALUES(order_num_android),
    gmv_android = VALUES(gmv_android),
    ticket_num_create = VALUES(ticket_num_create),
    order_num_create = VALUES(order_num_create),
    ticket_num_create_ios = VALUES(ticket_num_create_ios),
    ticket_num_create_android = VALUES(ticket_num_create_android),
    order_num_create_ios = VALUES(order_num_create_ios),
    order_num_create_android = VALUES(order_num_create_android),
    gmv_create = VALUES(gmv_create),
    gmv_create_ios = VALUES(gmv_create_ios),
    gmv_create_android = VALUES(gmv_create_android)
"""


gtgj_order_hourly = """
    select CONCAT(HOUR(pay_time)) hm,
    DATE_FORMAT(pay_time,'%%Y-%%m-%%d') s_day,
    count(*) order_num,
    sum(ticket_count) ticket_num,
    count(DISTINCT uid) consumers,
    count(distinct case when p_info LIKE '%%ios%%' then uid else null end ) consumers_ios,
    count(distinct case when p_info LIKE '%%android%%' then uid else null end ) consumers_android
    from user_order
    where pay_time>= %s
    and pay_time< %s
    and i_status=3
    group by hm
    ORDER BY pay_time
"""

query_gtgj_order_by_hour = """
    select * from gtgj_order_hourly where hour=%s and s_day=%s
"""

insert_gtgj_order_hourly ="""
    insert into gtgj_order_hourly (hour, s_day, order_num, ticket_num, consumers, consumers_ios , consumers_android,
    createtime, updatetime) values (%s, %s, %s, %s, %s, %s, %s, now(), now())
"""

update_gtgj_order_hourly = """
    update gtgj_order_hourly set order_num=%s,ticket_num=%s, consumers=%s, consumers_ios=%s, consumers_android=%s,
    updatetime=now() where hour=%s and s_day=%s
"""

query_gtgj_order_from_hb = """
    SELECT DATE_FORMAT(buy_success_time, '%%Y-%%m-%%d') s_day,
    count(1) order_count
    FROM platformbuy_statistics_list
    WHERE  buy_success_time >= %s
    and buy_success_time < %s
    AND platform_id='200'
"""

query_gtgj_ticket_from_hb = """
    SELECT DATE_FORMAT(buy_success_time, '%%Y-%%m-%%d') s_day,
    count(1) ticket_count
    FROM platformbuy_statistics_table
    WHERE  buy_success_time >= %s
    and buy_success_time < %s
    AND platform_id='200'
"""

insert_gtgj_from_hb = """
    insert into gtgj_ticket_from_hb
    (s_day, order_num, ticket_num, createtime, updatetime) values (%s, %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    ticket_num = VALUES(ticket_num),
    order_num = VALUES(order_num)
"""

# insert_gtgj_from_hb = """
#     insert into gtgj_ticket_from_hb
#     (s_day, ticket_num, createtime, updatetime) values (%s, %s, now(), now())
#     on duplicate key update updatetime = now() ,
#     ticket_num = VALUES(ticket_num)
# """