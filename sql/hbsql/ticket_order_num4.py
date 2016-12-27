ticket_order_num_sql = """
select DATE_FORMAT(TICKET_ORDER.CREATETIME, '%%Y-%%m-%%d') s_day,
count(TICKET_ORDER.ORDERID) ticket_num,
count(distinct TICKET_ORDER.ORDERID) order_num
from TICKET_ORDERDETAIL INNER JOIN
TICKET_ORDER ON TICKET_ORDER.ORDERID = TICKET_ORDERDETAIL.ORDERID
where TICKET_ORDER.ORDERSTATUE not in (2,12,21,51,75)
and TICKET_ORDER.createtime>=%s
GROUP BY s_day
"""

hb_ticket_test_sql = "select * from TICKET_ORDERDETAIL limit 0,5;"


target_order_ticket_sql = """
insert into ticket_order_test(s_day, ticket_num, order_num)
values( % s, % s, % s);"""