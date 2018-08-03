# -*- coding: utf-8 -*-
# hotel_orders_daily = """
# SELECT  %s  s_day,
# sum(1) order_count,
# sum(roomcount) room_count,
# sum(case when producttype=3 then roomcount Else TIMESTAMPDIFF(day,arrivedate, leavedate) * roomcount END) night_count
# from hotelorder
# where createtime>=%s
# and createtime<%s
# and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
# and gdsname='p2p'
# GROUP BY s_day
# """

hotel_orders_daily = """
select B.s_day,
if(B.order_count is null, 0, B.order_count) order_count,
if(B.room_count is null, 0, B.room_count) room_count,
if(B.night_count is null, 0, B.night_count) night_count,

if(A.order_count_p2p is null, 0, A.order_count_p2p) order_count_p2p,
if(A.room_count_p2p is null, 0, room_count_p2p) room_count_p2p,
if(A.night_count_p2p is null, 0, A.night_count_p2p) night_count_p2p
from (SELECT DATE_FORMAT(createtime, '%%Y-%%m-%%d') s_day,
sum(1) order_count_p2p,
sum(roomcount) room_count_p2p,
sum(case when producttype=3 then roomcount Else TIMESTAMPDIFF(day,arrivedate, leavedate) * roomcount END) night_count_p2p
from hotelorder
where createtime>=%s
and createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
and gdsname='p2p'
and phoneid != '12831915'
GROUP BY s_day) A
right JOIN (
SELECT DATE_FORMAT(createtime, '%%Y-%%m-%%d') s_day,
sum(1) order_count,
sum(roomcount) room_count,
sum(case when producttype=3 then roomcount Else TIMESTAMPDIFF(day,arrivedate, leavedate) * roomcount END) night_count
from hotelorder
where createtime>=%s
and createtime<%s
and phoneid != '12831915'
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
GROUP BY s_day
) B on B.s_day=A.s_day

"""


update_hotel_orders_daily = """
    insert into hotel_orders_daily values (%s, %s, %s, %s, %s, %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    order_count = VALUES(order_count),
    room_count = VALUES(room_count),
    night_count = VALUES(night_count),
    order_count_p2p = VALUES(order_count_p2p),
    room_count_p2p = VALUES(room_count_p2p),
    night_count_p2p = VALUES(night_count_p2p)
"""
#
# hotel_orders_daily_history = """
# select B.s_day,
# if(B.order_count is null, 0, B.order_count) order_count,
# if(B.room_count is null, 0, B.room_count) room_count,
# if(B.night_count is null, 0, B.night_count) night_count,
# if(A.order_count_p2p is null, 0, A.order_count_p2p) order_count_p2p,
# if(A.room_count_p2p is null, 0, room_count_p2p) room_count_p2p,
# if(A.night_count_p2p is null, 0, A.night_count_p2p) night_count_p2p
# from (SELECT DATE_FORMAT(createtime, '%%Y-%%m-%%d') s_day,
# sum(1) order_count_p2p,
# sum(roomcount) room_count_p2p,
# sum(case when producttype=3 then roomcount Else TIMESTAMPDIFF(day,arrivedate, leavedate) * roomcount END) night_count_p2p
# from hotelorder
# where createtime<%s
# and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
# and gdsname='p2p'
# GROUP BY s_day) A
# right JOIN (
# SELECT DATE_FORMAT(createtime, '%%Y-%%m-%%d') s_day,
# sum(1) order_count,
# sum(roomcount) room_count,
# sum(case when producttype=3 then roomcount Else TIMESTAMPDIFF(day,arrivedate, leavedate) * roomcount END) night_count
# from hotelorder
# where createtime<%s
# and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
# GROUP BY s_day
# ) B on B.s_day=A.s_day
# """

#>4.21
hotel_orders_daily_history = """
select B.s_day,
if(B.order_count is null, 0, B.order_count) order_count,
if(B.room_count is null, 0, B.room_count) room_count,
if(B.night_count is null, 0, B.night_count) night_count,

if(A.order_count_p2p is null, 0, A.order_count_p2p) order_count_p2p,
if(A.room_count_p2p is null, 0, room_count_p2p) room_count_p2p,
if(A.night_count_p2p is null, 0, A.night_count_p2p) night_count_p2p
from (SELECT DATE_FORMAT(createtime, '%%Y-%%m-%%d') s_day,
sum(1) order_count_p2p,
sum(roomcount) room_count_p2p,
sum(case when producttype=3 then roomcount Else TIMESTAMPDIFF(day,arrivedate, leavedate) * roomcount END) night_count_p2p
from hotelorder
where createtime>=%s
and createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
and gdsname='p2p'
GROUP BY s_day) A
right JOIN (
SELECT DATE_FORMAT(createtime, '%%Y-%%m-%%d') s_day,
sum(1) order_count,
sum(roomcount) room_count,
sum(case when producttype=3 then roomcount Else TIMESTAMPDIFF(day,arrivedate, leavedate) * roomcount END) night_count
from hotelorder
where createtime>=%s
and createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
GROUP BY s_day
) B on B.s_day=A.s_day
"""