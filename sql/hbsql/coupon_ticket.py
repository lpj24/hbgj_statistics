hbgj_use_coupon_sql = """
        select
        sum(case when TRADE_TYPE=1 THEN 1 ELSE 0 END) use_coupon_count_in,
        sum(case when TRADE_TYPE=1 THEN price ELSE 0 END) use_coupon_amount_in,
        sum(case when TRADE_TYPE=4 THEN 1 ELSE 0 END) use_coupon_count_return,
        sum(case when TRADE_TYPE=4 THEN price ELSE 0 END) use_coupon_amount_return
        from skyhotel.TRADE_RECORD
        where productid=0
        and PAYSOURCE LIKE '%%coupon%%'
        and createtime>=%s
        and createtime<%s
"""

hbgj_issue_coupon_sql = """
        select DATE_FORMAT(C.createtime,'%%Y-%%m-%%d') s_day,
        sum(case when CL.amount>1 then 1 else 0 end) issue_coupon_count,
        sum(case when CL.amount>1 then C.amount else 0 end) issue_coupon_amount,
        sum(case when CL.amount=1.0 then 1 else 0 end) issue_discount_coupon_count
        from account.coupon C
        left join account.coupon_list CL on C.coupon_id = CL.id
        where C.bindtype=1
        and C.createtime>=%s
        and C.createtime<%s
        group by s_day
        order by s_day
"""


insert_hbgj_coupon_sql = """
    insert into coupon_hbgj_ticket (s_day, issue_coupon_count, issue_coupon_amount, issue_discount_coupon_count,
    use_coupon_count_in, use_coupon_amount_in, use_coupon_count_return, use_coupon_amount_return,
    createtime, updatetime) values (%s, %s, %s, %s, %s, %s, %s, %s, now(), now())
    on duplicate key update updatetime = now(),
    issue_coupon_count = values(issue_coupon_count),
    issue_coupon_amount = values(issue_coupon_amount),
    issue_discount_coupon_count = values(issue_discount_coupon_count),
    use_coupon_count_in = values(use_coupon_count_in),
    use_coupon_amount_in = values(use_coupon_amount_in),
    use_coupon_count_return = values(use_coupon_count_return),
    use_coupon_amount_return = values(use_coupon_amount_return)

"""

gtgj_use_issue_coupon_sql = """
        SELECT A.s_day, B.issue_coupon_count, B.issue_coupon_amount, A.use_coupon_count,
        A.use_coupon_amount FROM (
        select DATE_FORMAT(use_time, '%%Y-%%m-%%d') s_day,
        sum(1) use_coupon_count,
        sum(amount) use_coupon_amount
        from return_cash_coupon
        where use_time>=%s
        and use_time<%s
        and use_type=2
        GROUP BY s_day) A
        left join (select DATE_FORMAT(create_time, '%%Y-%%m-%%d') s_day,
        sum(1) issue_coupon_count,
        sum(amount) issue_coupon_amount
        from return_cash_coupon
        where create_time>=%s
        and create_time<%s
        GROUP BY s_day) B ON A.s_day = B.s_day
"""

insert_gtgj_coupon_sql = """
    insert into coupon_gtgj_ticket (s_day, issue_coupon_count, issue_coupon_amount, use_coupon_count
        ,use_coupon_amount, createtime, updatetime)
        values (%s, %s, %s, %s, %s, now(), now())
    on duplicate key update updatetime = now(),
    issue_coupon_count = values(issue_coupon_count),
    issue_coupon_amount = values(issue_coupon_amount),
    use_coupon_count = values(use_coupon_count),
    use_coupon_amount = values(use_coupon_amount)
"""

huoli_car_use_coupon_sql = """
    select
    sum(case when TRADE_TYPE=1 THEN 1 ELSE 0 END) use_coupon_count_in,
    sum(case when TRADE_TYPE=1 THEN price ELSE 0 END) use_coupon_amount_in,
    sum(case when TRADE_TYPE=4 THEN 1 ELSE 0 END) use_coupon_count_return,
    sum(case when TRADE_TYPE=4 THEN price ELSE 0 END) use_coupon_amount_return
    from skyhotel.TRADE_RECORD
    where productid=7
    and PAYSOURCE LIKE '%%coupon%%'
    and createtime>=%s
    and createtime<%s
"""

huoli_car_issue_coupon_sql = """
    select DATE_FORMAT(C.createtime, '%%Y-%%m-%%d') s_day,
    sum(case when CL.amount>1 then 1 else 0 end) issue_coupon_count,
    sum(case when CL.amount>1 then C.amount else 0 end) issue_coupon_amount,
    sum(case when CL.amount=1.0 then 1 else 0 end) issue_discount_coupon_count
    from account.coupon C
    left join account.coupon_list CL on C.coupon_id = CL.id
    where C.bindtype=7
    and C.createtime>=%s
    and C.createtime<%s
    group by s_day
    order by s_day
"""

insert_huoli_car_sql = """
    insert into coupon_huoli_car (s_day, issue_coupon_count, issue_coupon_amount, issue_discount_coupon_count,
    use_coupon_count_in, use_coupon_amount_in, use_coupon_count_return, use_coupon_amount_return,
    createtime, updatetime) values (%s, %s, %s, %s, %s, %s, %s, %s, now(), now())
    on duplicate key update updatetime = now(),
    issue_coupon_count = values(issue_coupon_count),
    issue_coupon_amount = values(issue_coupon_amount),
    issue_discount_coupon_count = values(issue_discount_coupon_count),
    use_coupon_count_in = values(use_coupon_count_in),
    use_coupon_amount_in = values(use_coupon_amount_in),
    use_coupon_count_return = values(use_coupon_count_return),
    use_coupon_amount_return = values(use_coupon_amount_return)
"""

huoli_hotel_use_coupon_sql = """
    select
    sum(case when TRADE_TYPE=1 THEN 1 ELSE 0 END) use_coupon_count_in,
    sum(case when TRADE_TYPE=1 THEN price ELSE 0 END) use_coupon_amount_in,
    sum(case when TRADE_TYPE=4 THEN 1 ELSE 0 END) use_coupon_count_return,
    sum(case when TRADE_TYPE=4 THEN price ELSE 0 END) use_coupon_amount_return
    from skyhotel.TRADE_RECORD
    where productid=36
    and PAYSOURCE LIKE '%%coupon%%'
    and createtime>=%s
    and createtime<%s
"""

huoli_hotel_issue_coupon_sql = """
    select
    sum(case when CL.amount>1 then 1 else 0 end) issue_coupon_count,
    sum(case when CL.amount>1 then C.amount else 0 end) issue_coupon_amount,
    sum(case when CL.amount=1.0 then 1 else 0 end) issue_discount_coupon_count
    from account.coupon C
    left join account.coupon_list CL on C.coupon_id = CL.id
    where C.bindtype=9
    and C.createtime>=%s
    and C.createtime<%s
"""

insert_huoli_hotel_sql = """
    insert into coupon_huoli_hotel (s_day, issue_coupon_count, issue_coupon_amount, issue_discount_coupon_count,
    use_coupon_count_in, use_coupon_amount_in, use_coupon_count_return, use_coupon_amount_return,
    createtime, updatetime) values (%s, %s, %s, %s, %s, %s, %s, %s, now(), now())
    on duplicate key update updatetime = now(),
    issue_coupon_count = values(issue_coupon_count),
    issue_coupon_amount = values(issue_coupon_amount),
    issue_discount_coupon_count = values(issue_discount_coupon_count),
    use_coupon_count_in = values(use_coupon_count_in),
    use_coupon_amount_in = values(use_coupon_amount_in),
    use_coupon_count_return = values(use_coupon_count_return),
    use_coupon_amount_return = values(use_coupon_amount_return)
"""

common_coupon_sql = """
    select DATE_FORMAT(C.createtime,'%%Y-%%m-%%d') s_day,
    sum(case when CL.amount>1 then 1 else 0 end) issue_coupon_count,
    sum(case when CL.amount>1 then C.amount else 0 end) issue_coupon_amount,
    sum(case when CL.amount=1.0 then 1 else 0 end) issue_discount_coupon_count
    from account.coupon C
    left join account.coupon_list CL on C.coupon_id = CL.id
    where C.bindtype=0
    and C.createtime>=%s
    and C.createtime<%s
    group by s_day
    order by s_day
"""

insert_common_coupon_sql = """
    insert into coupon_common (s_day, issue_coupon_count, issue_coupon_amount, issue_discount_coupon_count,
    createtime, updatetime) values (%s, %s, %s, %s, now(), now())
    on duplicate key update updatetime = now(),
    issue_coupon_count = values(issue_coupon_count),
    issue_coupon_amount = values(issue_coupon_amount),
    issue_discount_coupon_count = values(issue_discount_coupon_count)
"""

hbdj_use_detail_sql = """
    select B_A.s_day,  B_A.platform_type, B_A.coupon_id,
    sum(use_coupon_count_in) use_coupon_count_in,
    sum(use_coupon_amount_in) use_coupon_amount_in,
    sum(use_coupon_count_return) use_coupon_count_return,
    sum(use_coupon_amount_return) use_coupon_amount_return
     from (
    select s_day, coupon_id,
    case when platform like '%%gtgj%%'
    then 'gtgj' else 'hbgj' end platform_type,
    sum(case when TRADE_TYPE=1 then 1 else 0 END) use_coupon_count_in,
    sum(case when TRADE_TYPE=1 then price ELSE 0 END) use_coupon_amount_in,
    sum(case when TRADE_TYPE=4 then 1 ELSE 0 END) use_coupon_count_return,
    sum(case when TRADE_TYPE=4 then price ELSE 0 END) use_coupon_amount_return
    from
    (select DATE_FORMAT(TR.createtime, '%%Y-%%m-%%d') s_day,
    price, TRADE_TYPE,coupon_id, TOR.P platform
    from skyhotel.TRADE_RECORD TR
    left join account.coupon C on TR.COUPONID = C.cid
    left join skyhotel.TICKET_ORDER TOR on TR.ORDERID = TOR.ORDERID
    where productid=0
    and TR.PAYSOURCE LIKE '%%coupon%%'
    and TR.createtime>=%s
    and TR.createtime<%s
    and (TRADE_TYPE = 1 or TRADE_TYPE=4)
    ) as A
    group by s_day, coupon_id, platform
    order by s_day, use_coupon_count_in desc) B_A GROUP by B_A.s_day, B_A.coupon_id, B_A.platform_type
"""

insert_hbgj_use_detail_sql = """
    insert into coupon_hbgj_ticket_use_detail_client (s_day, client, coupon_id, use_coupon_count_in,
    use_coupon_amount_in, use_coupon_count_return, use_coupon_amount_return,
    createtime, updatetime) values (%s, %s, %s, %s, %s, %s, %s, now(), now())
"""

hbdj_use_detail_noclient_sql = """
        select s_day, coupon_id,
        sum(case when TRADE_TYPE=1 then 1 else 0 END) use_coupon_count_in,
        sum(case when TRADE_TYPE=1 then price ELSE 0 END) use_coupon_amount_in,
        sum(case when TRADE_TYPE=4 then 1 ELSE 0 END) use_coupon_count_return,
        sum(case when TRADE_TYPE=4 then price ELSE 0 END) use_coupon_amount_return
        from
        (select DATE_FORMAT(TR.createtime, '%%Y-%%m-%%d') s_day,
        price, TRADE_TYPE,coupon_id
        from skyhotel.TRADE_RECORD TR
        left join account.coupon C on TR.COUPONID = C.cid
        where productid=0
        and PAYSOURCE LIKE '%%coupon%%'
        and TR.createtime>=%s
        and TR.createtime<%s
        and (TRADE_TYPE = 1 or TRADE_TYPE=4)
        ) as A
        group by s_day, coupon_id
        order by s_day, use_coupon_count_in desc
"""

insert_hbgj_use_detail_noclient_sql = """
    insert into coupon_hbgj_ticket_use_detail (s_day, coupon_id, use_coupon_count_in,
    use_coupon_amount_in, use_coupon_count_return, use_coupon_amount_return,
    createtime, updatetime) values (%s, %s, %s, %s, %s, %s, now(), now())
"""

coupon_issue_detail_sql = """
    select DATE_FORMAT(C.createtime, '%%Y-%%m-%%d') s_day,
    C.bindtype,coupon_id,
    sum(case when CL.amount>1 then 1 else 0 end) issue_coupon_count,
    sum(case when CL.amount>1 then C.amount else 0 end) issue_coupon_amount,
    sum(case when CL.amount=1.0 then 1 else 0 end) issue_discount_coupon_count
    from account.coupon C
    left join account.coupon_list CL on C.coupon_id = CL.id
    where C.createtime>=%s
    and C.createtime<%s
    group by s_day, bindtype,coupon_id
    order by s_day,bindtype, issue_coupon_count desc
"""

insert_coupon_issue_detail_sql = """
    insert into coupon_issue_detail (s_day, bindtype, coupon_id, issue_coupon_count,
    issue_coupon_amount, issue_discount_coupon_count,
    createtime, updatetime) values (%s, %s, %s, %s, %s, %s, now(), now())

"""


huoli_car_coupon_detail_sql = """
    select s_day, coupon_id,
    sum(case when TRADE_TYPE=1 then 1 else 0 END) use_coupon_count_in,
    sum(case when TRADE_TYPE=1 then price ELSE 0 END) use_coupon_amount_in,
    sum(case when TRADE_TYPE=4 then 1 ELSE 0 END) use_coupon_count_return,
    sum(case when TRADE_TYPE=4 then price ELSE 0 END) use_coupon_amount_return
    from
    (select DATE_FORMAT(TR.createtime, '%%Y-%%m-%%d') s_day,
    price, TRADE_TYPE,coupon_id
    from skyhotel.TRADE_RECORD TR
    left join account.coupon C on TR.COUPONID = C.cid
    where productid=7
    and PAYSOURCE LIKE '%%coupon%%'
    and TR.createtime>=%s
    and TR.createtime<%s
    and (TRADE_TYPE = 1 or TRADE_TYPE=4)
    ) as A
    group by s_day, coupon_id
    order by s_day, use_coupon_count_in desc

"""

insert_coupon_car_use_detail_sql = """
    insert into coupon_huoli_car_use_detail (s_day, coupon_id, use_coupon_count_in,
    use_coupon_amount_in, use_coupon_count_return, use_coupon_amount_return,
    createtime, updatetime) values (%s, %s, %s, %s, %s, %s, now(), now())
"""

huoli_hotel_use_detail_sql = """
    select s_day, coupon_id,
    sum(case when TRADE_TYPE=1 then 1 else 0 END) use_coupon_count_in,
    sum(case when TRADE_TYPE=1 then price ELSE 0 END) use_coupon_amount_in,
    sum(case when TRADE_TYPE=4 then 1 ELSE 0 END) use_coupon_count_return,
    sum(case when TRADE_TYPE=4 then price ELSE 0 END) use_coupon_amount_return
    from
    (select DATE_FORMAT(TR.createtime, '%%Y-%%m-%%d') s_day,
    price, TRADE_TYPE,coupon_id
    from skyhotel.TRADE_RECORD TR
    left join account.coupon C on TR.COUPONID = C.cid
    where productid=36
    and PAYSOURCE LIKE '%%coupon%%'
    and TR.createtime>=%s
    and TR.createtime<%s
    and (TRADE_TYPE = 1 or TRADE_TYPE=4)
    ) as A
    group by s_day, coupon_id
    order by s_day, use_coupon_count_in desc
"""

insert_huoli_hotel_use_detail_sql = """
    insert into coupon_huoli_hotel_use_detail (s_day, coupon_id, use_coupon_count_in,
    use_coupon_amount_in, use_coupon_count_return, use_coupon_amount_return,
    createtime, updatetime) values (%s, %s, %s, %s, %s, %s, now(), now())
"""

gtgj_coupon_issue_detail_sql = """
    select DATE_FORMAT(create_time, '%%Y-%%m-%%d') s_day,
    title,
    sum(1) issue_coupon_count,
    sum(amount) issue_coupon_amount
    from return_cash_coupon
    where create_time>=%s
    and create_time<%s
    GROUP BY s_day, title
    order by s_day, issue_coupon_count desc
"""

gtgj_coupon_use_detail_sql = """
    select
    DATE_FORMAT(use_time, '%%Y-%%m-%%d') s_day,
    title,
    sum(case when use_type=2 then 1 else 0 end) use_coupon_count,
    sum(case when use_type=2 then amount else 0 end) use_coupon_amount
    from return_cash_coupon
    where use_time>=%s
    and use_time<%s
    GROUP BY s_day, title
    order by s_day, use_coupon_count desc
"""

insert_gtgj_coupon_issue_detail_sql = """
    insert into coupon_gtgj_ticket_issue_detail (s_day, title, issue_coupon_count,
    issue_coupon_amount,
    createtime, updatetime) values (%s, %s, %s, %s, now(), now())
"""

insert_gtgj_coupon_use_detail_sql = """
    insert into coupon_gtgj_ticket_use_detail (s_day, title, use_coupon_count,
    use_coupon_amount,
    createtime, updatetime) values (%s, %s, %s, %s, now(), now())

"""