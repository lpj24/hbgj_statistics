#coding:utf8
hotel_consumers_daily_his = """
select B.s_day,
if(B.consumers is null, 0, B.consumers) consumers,
if(A.consumers_p2p is null, 0, A.consumers_p2p) consumers_p2p
from (SELECT DATE_FORMAT(createtime, '%%Y-%%m-%%d') s_day,
count(DISTINCT phoneid) consumers_p2p
from hotelorder
where createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
and gdsname='p2p'
GROUP BY s_day) A
right JOIN (
SELECT DATE_FORMAT(createtime, '%%Y-%%m-%%d') s_day,
count(DISTINCT phoneid) consumers
from hotelorder
where createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
GROUP BY s_day
) B on B.s_day=A.s_day
"""

hotel_consumers_daily = """
select B.s_day,
if(B.consumers is null, 0, B.consumers) consumers,
if(A.consumers_p2p is null, 0, A.consumers_p2p) consumers_p2p
from (SELECT DATE_FORMAT(createtime, '%%Y-%%m-%%d') s_day,
count(DISTINCT phoneid) consumers_p2p
from hotelorder
where createtime>=%s
and createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
and gdsname='p2p'
GROUP BY s_day) A
right JOIN (
SELECT DATE_FORMAT(createtime, '%%Y-%%m-%%d') s_day,
count(DISTINCT phoneid) consumers
from hotelorder
where createtime>=%s
and createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
GROUP BY s_day
) B on B.s_day=A.s_day
"""

update_hotel_consumers_daily = """
    insert into hotel_consumers_daily values (%s, %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    consumers = VALUES(consumers),
    consumers_p2p = VALUES(consumers_p2p)
"""

hotel_consumers_weekly_his = """
select B.s_day,
if(B.consumers is null, 0, B.consumers) consumers,
if(A.consumers_p2p is null, 0, A.consumers_p2p) consumers_p2p
from (SELECT date_format(subdate(createtime,date_format(createtime,'%%w')-1),'%%Y-%%m-%%d') s_day,
count(DISTINCT phoneid) consumers_p2p
from hotelorder
where createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
and gdsname='p2p'
GROUP BY s_day) A
right JOIN (
SELECT date_format(subdate(createtime,date_format(createtime,'%%w')-1),'%%Y-%%m-%%d') s_day,
count(DISTINCT phoneid) consumers
from hotelorder
where createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
GROUP BY s_day
) B on B.s_day=A.s_day
"""

hotel_consumers_weekly = """
select B.s_day,
if(B.consumers is null, 0, B.consumers) consumers,
if(A.consumers_p2p is null, 0, A.consumers_p2p) consumers_p2p
from (SELECT date_format(subdate(createtime,date_format(createtime,'%%w')-1),'%%Y-%%m-%%d') s_day,
count(DISTINCT phoneid) consumers_p2p
from hotelorder
where createtime>=%s
and createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
and gdsname='p2p'
GROUP BY s_day) A
right JOIN (
SELECT date_format(subdate(createtime,date_format(createtime,'%%w')-1),'%%Y-%%m-%%d') s_day,
count(DISTINCT phoneid) consumers
from hotelorder
where createtime>=%s
and createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
GROUP BY s_day
) B on B.s_day=A.s_day
"""

update_hotel_consumers_weekly = """
    insert into hotel_consumers_weekly values (%s, %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    consumers = VALUES(consumers),
    consumers_p2p = VALUES(consumers_p2p)
"""

hotel_consumers_monthly_his = """
select B.s_day,
if(B.consumers is null, 0, B.consumers) consumers,
if(A.consumers_p2p is null, 0, A.consumers_p2p) consumers_p2p
from (SELECT DATE_FORMAT(str_to_date(CONCAT(YEAR(createtime),'-',MONTH(createtime),'-01'),'%%Y-%%m-%%d'),'%%Y-%%m-%%d') s_day,
count(DISTINCT phoneid) consumers_p2p
from hotelorder
where createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
and gdsname='p2p'
GROUP BY s_day) A
right JOIN (
SELECT DATE_FORMAT(str_to_date(CONCAT(YEAR(createtime),'-',MONTH(createtime),'-01'),'%%Y-%%m-%%d'),'%%Y-%%m-%%d') s_day,
count(DISTINCT phoneid) consumers
from hotelorder
where createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
GROUP BY s_day
) B on B.s_day=A.s_day
"""

hotel_consumers_monthly = """
select B.s_day,
if(B.consumers is null, 0, B.consumers) consumers,
if(A.consumers_p2p is null, 0, A.consumers_p2p) consumers_p2p
from (SELECT DATE_FORMAT(str_to_date(CONCAT(YEAR(createtime),'-',MONTH(createtime),'-01'),'%%Y-%%m-%%d'),'%%Y-%%m-%%d') s_day,
count(DISTINCT phoneid) consumers_p2p
from hotelorder
where createtime>=%s
and createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
and gdsname='p2p'
GROUP BY s_day) A
right JOIN (
SELECT DATE_FORMAT(str_to_date(CONCAT(YEAR(createtime),'-',MONTH(createtime),'-01'),'%%Y-%%m-%%d'),'%%Y-%%m-%%d') s_day,
count(DISTINCT phoneid) consumers
from hotelorder
where createtime>=%s
and createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
GROUP BY s_day
) B on B.s_day=A.s_day
"""

update_hotel_consumers_monthly = """
    insert into hotel_consumers_monthly values (%s, %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    consumers = VALUES(consumers),
    consumers_p2p = VALUES(consumers_p2p)
"""