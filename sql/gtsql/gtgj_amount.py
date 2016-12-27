#coding:utf8
gtgj_change_oids = """
    select distinct(order_id) oid from user_sub_order where create_time >=%s and
    create_time < %s and ( status='改签票' or
    ( status='已退票' and change_time is not null and change_time!=refund_time ))
"""

gtgj_change_info = """
    select card_no, change_time,price, status ,refund_time from user_sub_order where order_id = %s
"""

gtgj_amount_success = """
    select sum(succ_amount) from global_statistics where s_day>=%s and s_day<%s group by s_day
"""

gtgj_amount_success_daily = """
    select gmv from gtgj_order_daily where s_day >= %s and s_day<%s
"""

gtgj_amount_create = """
    select gmv_create from gtgj_order_daily where s_day >= %s and s_day<%s
"""

gtgj_amount_grab = """
    select (prepaid_amount-success_amount) from gtgj_qp_daily where s_day >= %s and s_day<%s
"""

update_gtgj_amount_daily = """
    insert into gtgj_amount_daily values (%s, %s , %s, %s,%s, now(), now())
    on duplicate key update updatetime = now() ,
    success_amount = VALUES(success_amount),
    change_amount = VALUES(change_amount),
    create_amount = VALUES(create_amount),
    grab_amount = VALUES(grab_amount)
"""