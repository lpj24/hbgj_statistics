hb_orders_daily = """
select TicketAll.s_day,TicketAll.ticket_num,TicketAll.order_num,Ticket_GT.ticket_num_gt,Ticket_GT.order_num_gt
      from
      (SELECT DATE_FORMAT(CREATETIME,'%%Y-%%m-%%d') s_day,
      count(A.ORDERID) ticket_num,
      count(distinct A.ORDERID) order_num,
      count(case when A.p like '%%gtgj%%' then A.ORDERID END) ticket_num_gt
      from
      (select TICKET_ORDERDETAIL.createtime as CREATETIME,TICKET_ORDERDETAIL.ORDERID as ORDERID,TICKET_ORDER.p AS p
      from TICKET_ORDERDETAIL INNER JOIN  TICKET_ORDER ON TICKET_ORDER.ORDERID = TICKET_ORDERDETAIL.ORDERID
      where TICKET_ORDER.ORDERSTATUE not in (2,12,21,51,75)
      and TICKET_ORDER.createtime>=%s
      and TICKET_ORDER.createtime<%s) as A
      GROUP BY s_day) as TicketAll,
      (SELECT DATE_FORMAT(CREATETIME,'%%Y-%%m-%%d') s_day,
      count(A.ORDERID) ticket_num_gt,
      count(distinct A.ORDERID) order_num_gt
      from
      (select TICKET_ORDERDETAIL.createtime as CREATETIME,TICKET_ORDERDETAIL.ORDERID as ORDERID,TICKET_ORDER.p AS p
      from TICKET_ORDERDETAIL INNER JOIN  TICKET_ORDER ON TICKET_ORDER.ORDERID = TICKET_ORDERDETAIL.ORDERID
      where TICKET_ORDER.ORDERSTATUE not in (2,12,21,51,75)
      and TICKET_ORDER.createtime>=%s
      and TICKET_ORDER.createtime<%s) as A
      where A.p like '%%gtgj%%'
      GROUP BY s_day
      ) as Ticket_GT
      where TicketAll.s_day = Ticket_GT.s_day

"""

update_hb_orders_daily = """
    insert into hbgj_order_daily (s_day, ticket_num, order_num, ticket_num_gt, order_num_gt,
    createtime, updatetime) values (%s, %s , %s, %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    ticket_num = VALUES(ticket_num),
    order_num = VALUES(order_num),
    ticket_num_gt = VALUES(ticket_num_gt),
    order_num_gt = VALUES(order_num_gt)

"""


