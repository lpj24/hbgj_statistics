hb_order_ticket_sql = """
    SELECT DATE_FORMAT(TICKET_ORDER.CREATETIME,'%%Y-%%m-%%d') s_day,
    count(distinct TICKET_ORDERDETAIL.ORDERID) order_num,
    count(TICKET_ORDERDETAIL.ORDERID) ticket_num,
    count(distinct case when TICKET_ORDER.p like '%%gtgj%%' then TICKET_ORDERDETAIL.ORDERID END) order_num_gt,
    count(case when TICKET_ORDER.p like '%%gtgj%%' then TICKET_ORDERDETAIL.ORDERID END) ticket_num_gt,

    count(DISTINCT case when TICKET_ORDER.INTFLAG=0 then TICKET_ORDERDETAIL.ORDERID END) inland_order_num,
    count(case when TICKET_ORDER.INTFLAG=0 then TICKET_ORDERDETAIL.ORDERID END) inland_ticket_num,
    count(DISTINCT case when TICKET_ORDER.INTFLAG=1 then TICKET_ORDERDETAIL.ORDERID END) inter_order_num,
    count(case when TICKET_ORDER.INTFLAG=1 then TICKET_ORDERDETAIL.ORDERID END) inter_ticket_num
    FROM TICKET_ORDERDETAIL
    join TICKET_ORDER
    ON TICKET_ORDER.ORDERID = TICKET_ORDERDETAIL.ORDERID
    where TICKET_ORDER.ORDERSTATUE not in (2,12,21,51,75)
    and  TICKET_ORDER.CREATETIME >= %s
    and  TICKET_ORDER.CREATETIME < %s
    GROUP BY s_day
    order by s_day
"""

update_hb_order_ticket_sql = """
    insert into hb_gt_order_daily (s_day, order_num, ticket_num, order_num_gt, ticket_num_gt
        ,inland_order_num, inland_ticket_num, inter_order_num, inter_ticket_num,
        createtime, updatetime
    ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
    on duplicate key update updatetime = now(),
    s_day = values(s_day),
    order_num = values(order_num),
    ticket_num = values(ticket_num),
    order_num_gt = values(order_num_gt),
    ticket_num_gt = values(ticket_num_gt),
    inland_order_num = values(inland_order_num),
    inland_ticket_num = values(inland_ticket_num),
    inter_order_num = values(inter_order_num),
    inter_ticket_num = values(inter_ticket_num)
"""