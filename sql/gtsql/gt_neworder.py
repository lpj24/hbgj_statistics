#coding:utf8

gt_neworder_daily = """
    select
    AA.s_day, AA.ticket_count ticket_num, AA_O.order_count order_num, AA.amount gmv,
    CC.ticket_count_ios ticket_num_ios, CC_O.order_count order_num_ios, CC.amount_ios gmv_ios,
    BB.ticket_count_android ticket_num_android, BB_O.order_count order_num_android, BB.amount_android gmv_android
     from (
    select DATE_FORMAT(create_time, '%%Y-%%m-%%d') s_day, count(*) ticket_count,
    sum(price) amount
    from user_sub_order
    where  create_time>=%s
    and create_time<%s
    and status not in ('取消订单','取消改签')
    GROUP BY s_day) AA
    left JOIN (
    select DATE_FORMAT(create_time, '%%Y-%%m-%%d') s_day, count(*) ticket_count_android,
    sum(price) amount_android
    from user_sub_order
    where  create_time>=%s
    and create_time<%s
    and status not in ('取消订单','取消改签')
    and p_info like '%%android%%'
    GROUP BY s_day) BB on AA.s_day = BB.s_day
    LEFT JOIN (
    select DATE_FORMAT(create_time, '%%Y-%%m-%%d') s_day, count(*) ticket_count_ios,
    sum(price) amount_ios
    from user_sub_order
    where  create_time>=%s
    and create_time<%s
    and status not in ('取消订单','取消改签')
    and p_info like '%%ios%%'
    GROUP BY s_day) CC on BB.s_day = CC.s_day
LEFT JOIN  (
	select DATE_FORMAT(create_time, '%%Y-%%m-%%d') s_day,
    count(distinct order_id) order_count
    from user_order
    where  create_time>=%s
    and create_time<%s
    and i_status!=2
    GROUP BY s_day) AA_O on CC.s_day=AA_O.s_day
left join (
select DATE_FORMAT(create_time, '%%Y-%%m-%%d') s_day,
    count(distinct order_id) order_count
    from user_order
    where  create_time>=%s
    and create_time<%s
    and i_status!=2
	and p_info like '%%android%%'
    GROUP BY s_day
) BB_O on AA_O.s_day = BB_O.s_day
left join (
	select DATE_FORMAT(create_time, '%%Y-%%m-%%d') s_day,
    count(distinct order_id) order_count
    from user_order
    where  create_time>=%s
    and create_time<%s
    and i_status!=2
	and p_info like '%%ios%%'
    GROUP BY s_day
) CC_O on BB_O.s_day = CC_O.s_day

"""

'''
select
A.s_day, A.ticket_num, B.order_num, A.gmv,
A.ticket_num_ios, B.order_num_ios, A.gmv_ios,
A.ticket_num_android, B.order_num_android, A.gmv_android
 from (
   select DATE_FORMAT(create_time, '%%Y-%%m-%%d') s_day, count(*) ticket_num,
    sum(price) gmv,
    count(case when p_info like '%%android%%' then order_id end) ticket_num_android ,
    sum(case when p_info like '%%android%%' then price end) gmv_android,
    count(case when p_info like '%%ios%%' then order_id end) ticket_num_ios,
    sum(case when p_info like '%%ios%%' then price end) gmv_ios
    from user_sub_order
    where  create_time>=%s
    and create_time<%s
    and status not in ('取消订单','取消改签')
    GROUP BY s_day) A
LEFT JOIN (select DATE_FORMAT(create_time, '%%Y-%%m-%%d') s_day,
    count(distinct order_id) order_num,
    count(case when p_info like '%%android%%' then order_id end) order_num_android,
    count(case when p_info like '%%android%%' then order_id end) order_num_ios
    from user_order
    where  create_time>=%s
    and create_time<%s
    and i_status!=2
    GROUP BY s_day) B ON A.s_day = B.s_day
'''

update_gtgj_new_order_daily = """
    insert into gtgj_new_order_daily
    (s_day,ticket_num,order_num,gmv,
    ticket_num_ios,order_num_ios,gmv_ios,
    ticket_num_android,order_num_android,gmv_android,
    createtime, updatetime
    ) values (%s, %s , %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    ticket_num = VALUES(ticket_num),
    order_num = VALUES(order_num),
    gmv = VALUES(gmv),
    ticket_num_ios = VALUES(ticket_num_ios),
    order_num_ios = VALUES(order_num_ios),
    gmv_ios = VALUES(gmv_ios),
    ticket_num_android = VALUES(ticket_num_android),
    order_num_android = VALUES(order_num_android),
    gmv_android = VALUES(gmv_android)
"""

gt_neworder_order_daily = """
    select AA.s_day, AA.order_count order_num, BB.order_count order_num_android, CC.order_count order_num_ios
    from (select DATE_FORMAT(create_time, '%%Y-%%m-%%d') s_day,
    count(distinct order_id) order_count
    from user_order
    where  create_time>=%s
    and create_time<%s
    and i_status!=2
    GROUP BY s_day) AA
    left join (select DATE_FORMAT(create_time, '%%Y-%%m-%%d') s_day,
    count(distinct order_id) order_count
    from user_order
    where  create_time>=%s
    and create_time<%s
    and i_status!=2
    and p_info like '%%android%%'
    GROUP BY s_day) BB on AA.s_day = BB.s_day
left join (select DATE_FORMAT(create_time, '%%Y-%%m-%%d') s_day,
    count(distinct order_id) order_count
    from user_order
    where  create_time>=%s
    and create_time<%s
    and i_status!=2
    and p_info like '%%ios%%'
    GROUP BY s_day
) CC on CC.s_day = CC.s_day
"""