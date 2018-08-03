car_consumers_daily = """
SELECT A.s_day,
    A.consumers_jj, A.consumers_ios_jj, A.consumers_android_jj,
B.consumers_sj, B.consumers_ios_sj, B.consumers_android_sj,
    C.consumers_jz, C.consumers_ios_jz, C.consumers_android_jz,
D.consumers_sz, D.consumers_ios_sz, D.consumers_android_sz
FROM (
SELECT %s s_day,
COUNT(distinct phone_id) consumers_jj,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_jj,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_jj
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=1) A
left join (
SELECT %s s_day,
COUNT(distinct phone_id) consumers_sj,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_sj,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_sj
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=2) B on A.s_day = B.s_day
LEFT JOIN
(
SELECT %s s_day,
COUNT(distinct phone_id) consumers_jz,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_jz,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_jz
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=5) C on B.s_day = C.s_day
  LEFT JOIN
(
SELECT %s s_day,
COUNT(distinct phone_id) consumers_sz,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_sz,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_sz
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=6) D on C.s_day = D.s_day
"""
update_car_consumers_daily = """
    insert into huoli_car_consumers_daily values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,now(), now())
    on duplicate key update updatetime = now(),
    consumers_jj = VALUES(consumers_jj),
    consumers_ios_jj =  VALUES(consumers_ios_jj),
    consumers_android_jj =  VALUES(consumers_android_jj),
    consumers_sj = VALUES(consumers_sj),
    consumers_ios_sj =  VALUES(consumers_ios_sj),
    consumers_android_sj =  VALUES(consumers_android_sj),
    consumers_jz = VALUES(consumers_jz),
    consumers_ios_jz =  VALUES(consumers_ios_jz),
    consumers_android_jz =  VALUES(consumers_android_jz),
    consumers_sz = VALUES(consumers_sz),
    consumers_ios_sz =  VALUES(consumers_ios_sz),
    consumers_android_sz =  VALUES(consumers_android_sz)
"""

car_consumers_weekly = """
SELECT A.s_day,
    A.consumers_jj, A.consumers_ios_jj, A.consumers_android_jj,
B.consumers_sj, B.consumers_ios_sj, B.consumers_android_sj,
    C.consumers_jz, C.consumers_ios_jz, C.consumers_android_jz,
D.consumers_sz, D.consumers_ios_sz, D.consumers_android_sz
FROM (
SELECT %s s_day,
COUNT(distinct phone_id) consumers_jj,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_jj,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_jj
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=1) A
left join (
SELECT %s s_day,
COUNT(distinct phone_id) consumers_sj,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_sj,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_sj
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=2) B on A.s_day = B.s_day
LEFT JOIN
(
SELECT %s s_day,
COUNT(distinct phone_id) consumers_jz,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_jz,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_jz
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=5) C on B.s_day = C.s_day
  LEFT JOIN
(
SELECT %s s_day,
COUNT(distinct phone_id) consumers_sz,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_sz,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_sz
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=6) D on C.s_day = D.s_day
"""

update_car_consumers_weekly = """
    insert into huoli_car_consumers_weekly values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,now(), now())
    on duplicate key update updatetime = now(),
    consumers_jj = VALUES(consumers_jj),
    consumers_ios_jj =  VALUES(consumers_ios_jj),
    consumers_android_jj =  VALUES(consumers_android_jj),
    consumers_sj = VALUES(consumers_sj),
    consumers_ios_sj =  VALUES(consumers_ios_sj),
    consumers_android_sj =  VALUES(consumers_android_sj),
    consumers_jz = VALUES(consumers_jz),
    consumers_ios_jz =  VALUES(consumers_ios_jz),
    consumers_android_jz =  VALUES(consumers_android_jz),
    consumers_sz = VALUES(consumers_sz),
    consumers_ios_sz =  VALUES(consumers_ios_sz),
    consumers_android_sz =  VALUES(consumers_android_sz)
"""


car_consumers_monthly = """
SELECT A.s_day,
    A.consumers_jj, A.consumers_ios_jj, A.consumers_android_jj,
B.consumers_sj, B.consumers_ios_sj, B.consumers_android_sj,
    C.consumers_jz, C.consumers_ios_jz, C.consumers_android_jz,
D.consumers_sz, D.consumers_ios_sz, D.consumers_android_sz
FROM (
SELECT %s s_day,
COUNT(distinct phone_id) consumers_jj,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_jj,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_jj
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=1) A
left join (
SELECT %s s_day,
COUNT(distinct phone_id) consumers_sj,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_sj,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_sj
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=2) B on A.s_day = B.s_day
LEFT JOIN
(
SELECT %s s_day,
COUNT(distinct phone_id) consumers_jz,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_jz,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_jz
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=5) C on B.s_day = C.s_day
  LEFT JOIN
(
SELECT %s s_day,
COUNT(distinct phone_id) consumers_sz,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_sz,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_sz
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=6) D on C.s_day = D.s_day
"""

update_car_consumers_monthly = """
    insert into huoli_car_consumers_weekly values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,now(), now())
    on duplicate key update updatetime = now(),
    consumers_jj = VALUES(consumers_jj),
    consumers_ios_jj =  VALUES(consumers_ios_jj),
    consumers_android_jj =  VALUES(consumers_android_jj),
    consumers_sj = VALUES(consumers_sj),
    consumers_ios_sj =  VALUES(consumers_ios_sj),
    consumers_android_sj =  VALUES(consumers_android_sj),
    consumers_jz = VALUES(consumers_jz),
    consumers_ios_jz =  VALUES(consumers_ios_jz),
    consumers_android_jz =  VALUES(consumers_android_jz),
    consumers_sz = VALUES(consumers_sz),
    consumers_ios_sz =  VALUES(consumers_ios_sz),
    consumers_android_sz =  VALUES(consumers_android_sz)
"""


car_consumers_quarterly = """
SELECT A.s_day,
A.consumers_jj, A.consumers_ios_jj, A.consumers_android_jj,
B.consumers_sj, B.consumers_ios_sj, B.consumers_android_sj,
C.consumers_jz, C.consumers_ios_jz, C.consumers_android_jz,
D.consumers_sz, D.consumers_ios_sz, D.consumers_android_sz
FROM (
SELECT CONCAT(YEAR(%s),',','Q',QUARTER(%s)) s_day,
COUNT(distinct phone_id) consumers_jj,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_jj,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_jj
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=1) A
left join (
SELECT CONCAT(YEAR(%s),',','Q',QUARTER(%s)) s_day,
COUNT(distinct phone_id) consumers_sj,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_sj,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_sj
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=2) B on A.s_day = B.s_day
LEFT JOIN
(
SELECT CONCAT(YEAR(%s),',','Q',QUARTER(%s)) s_day,
COUNT(distinct phone_id) consumers_jz,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_jz,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_jz
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=5) C on B.s_day = C.s_day
  LEFT JOIN
(
SELECT CONCAT(YEAR(%s),',','Q',QUARTER(%s)) s_day,
COUNT(distinct phone_id) consumers_sz,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_sz,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_sz
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=6) D on C.s_day = D.s_day
"""

update_car_consumers_quarterly = """
    insert into huoli_car_consumers_quarterly values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,now(), now())
    on duplicate key update updatetime = now(),
    consumers_jj = VALUES(consumers_jj),
    consumers_ios_jj =  VALUES(consumers_ios_jj),
    consumers_android_jj =  VALUES(consumers_android_jj),
    consumers_sj = VALUES(consumers_sj),
    consumers_ios_sj =  VALUES(consumers_ios_sj),
    consumers_android_sj =  VALUES(consumers_android_sj),
    consumers_jz = VALUES(consumers_jz),
    consumers_ios_jz =  VALUES(consumers_ios_jz),
    consumers_android_jz =  VALUES(consumers_android_jz),
    consumers_sz = VALUES(consumers_sz),
    consumers_ios_sz =  VALUES(consumers_ios_sz),
    consumers_android_sz =  VALUES(consumers_android_sz)
"""

car_newconsumers_daily = """
    select A.s_day,
     C.new_consumers, C.new_consumers_ios, C.new_consumers_android, C.new_orders, C.new_orders_ios, C.new_orders_android,
    A.new_consumers_jsj, A.new_consumers_ios_jsj, A.new_consumers_android_jsj, A.new_orders_jsj, A.new_orders_ios_jsj, A.new_orders_android_jsj,
    B.new_consumers_jsz, B.new_consumers_ios_jsz, B.new_consumers_android_jsz, B.new_orders_jsz, B.new_orders_ios_jsz, B.new_orders_android_jsz
    from
    (SELECT %s s_day,
    COUNT(distinct phone_id) new_consumers,
    COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) new_consumers_ios,
    COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) new_consumers_android,
    COUNT(order_id) new_orders,
    COUNT(case when paramp like '%%ios%%' then order_id else null end) new_orders_ios,
    COUNT(case when paramp like '%%android%%' then order_id else null end) new_orders_android
    from orders
    where order_status in (11,13,14,18,31,32)
    and DATE_FORMAT(createtime,'%%Y-%%m-%%d')=%s
    and (
    case when createtime>="2014-09-26 00:00:00" then (get_type in (1,2,5,6))
    when createtime<"2014-09-26 00:00:00" then (get_type is null or get_type in (5,6)) end)
    and phone_id not in (
    select phone_id
    from orders
    where Date_format(createtime,'%%Y-%%m-%%d')<%s
    and order_status in (11,13,14,18,31,32)
    and (case when createtime>="2014-09-26 00:00:00" then (get_type in (1,2,5,6))
    when createtime<"2014-09-26 00:00:00" then (get_type is null) end))) C
    left join (
    SELECT %s s_day,
    COUNT(distinct phone_id) new_consumers_jsj,
    COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) new_consumers_ios_jsj,
    COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) new_consumers_android_jsj,
    COUNT(order_id) new_orders_jsj,
    COUNT(case when paramp like '%%ios%%' then order_id else null end) new_orders_ios_jsj,
    COUNT(case when paramp like '%%android%%' then order_id else null end) new_orders_android_jsj
    from orders
    where order_status in (11,13,14,18,31,32)
    and DATE_FORMAT(createtime,'%%Y-%%m-%%d')=%s
    and (
    case when createtime>="2014-09-26 00:00:00" then (get_type in (1,2))
    when createtime<"2014-09-26 00:00:00" then (get_type is null) end)
    and phone_id not in (
    select phone_id
    from orders
    where Date_format(createtime,'%%Y-%%m-%%d')<%s
    and order_status in (11,13,14,18,31,32)
    and (case when createtime>="2014-09-26 00:00:00" then (get_type in (1,2))
    when createtime<"2014-09-26 00:00:00" then (get_type is null) end))
     ) A on C.s_day=A.s_day
    LEFT JOIN
    (SELECT %s s_day,
    COUNT(distinct phone_id) new_consumers_jsz,
    COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) new_consumers_ios_jsz,
    COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) new_consumers_android_jsz,
    COUNT(order_id) new_orders_jsz,
    COUNT(case when paramp like '%%ios%%' then order_id else null end) new_orders_ios_jsz,
    COUNT(case when paramp like '%%android%%' then order_id else null end) new_orders_android_jsz
    from orders
    where order_status in (11,13,14,18,31,32)
    and get_type in (5,6)
    and DATE_FORMAT(createtime,'%%Y-%%m-%%d')=%s
    and phone_id not in (
    select phone_id
    from orders
    where Date_format(createtime,'%%Y-%%m-%%d')<%s
    and order_status in (11,13,14,18,31,32)
    and get_type in (5,6))
    ) B on A.s_day = B.s_day
"""

update_car_newconsumers_daily = """
    insert into huoli_car_newconsumers_daily values (%s, %s, %s, %s, %s,%s,%s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),

    new_consumers = VALUES(new_consumers),
    new_consumers_ios =  VALUES(new_consumers_ios),
    new_consumers_android =  VALUES(new_consumers_android),
    new_orders= VALUES(new_orders),
    new_orders_ios = VALUES(new_orders_ios),
    new_orders_android = VALUES(new_orders_android),

    new_consumers_jsj = VALUES(new_consumers_jsj),
    new_consumers_ios_jsj =  VALUES(new_consumers_ios_jsj),
    new_consumers_android_jsj =  VALUES(new_consumers_android_jsj),
    new_orders_jsj= VALUES(new_orders_jsj),
    new_orders_ios_jsj = VALUES(new_orders_ios_jsj),
    new_orders_android_jsj = VALUES(new_orders_android_jsj),

    new_consumers_jsz = VALUES(new_consumers_jsz),
    new_consumers_ios_jsz =  VALUES(new_consumers_ios_jsz),
    new_consumers_android_jsz =  VALUES(new_consumers_android_jsz),
    new_orders_jsz= VALUES(new_orders_jsz),
    new_orders_ios_jsz = VALUES(new_orders_ios_jsz),
    new_orders_android_jsz = VALUES(new_orders_android_jsz)
"""


##jiesongji
car_consumers_jz_daily = """
select A.s_day,
C.consumers, C.consumers_ios,C.consumers_android,
A.consumers_jsj, A.consumers_ios_jsj, A.consumers_android_jsj,
if(isnull(B.consumers_jsz), 0, B.consumers_jsz) consumers_jsz,
if(isnull(B.consumers_ios_jsz), 0, B.consumers_ios_jsz) consumers_ios_jsz,
if(isnull(B.consumers_android_jsz), 0, B.consumers_android_jsz) consumers_android_jsz
FROM
(SELECT %s s_day,
COUNT(distinct phone_id) consumers,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android
from orders
where createtime>=%s
and createtime<%s
and order_status not in (0,10)
and get_type in (1,2,5,6)) C
left join (
SELECT %s s_day,
COUNT(distinct phone_id) consumers_jsj,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_jsj,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_jsj
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type in (1,2)) A on C.s_day=A.s_day
left join (
SELECT %s s_day,
COUNT(distinct phone_id) consumers_jsz,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_jsz,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_jsz
from orders
where createtime>=%s
and createtime<%s
and order_status not in (0,10)
and get_type in (5,6)) B on A.s_day = B.s_day
"""

update_car_consumers_jz_daily = """
    insert into huoli_car_consumers_daily values (%s, %s ,%s,%s, %s ,%s, %s,%s, %s ,%s,  now(), now())
    on duplicate key update updatetime = now(),
    consumers = VALUES(consumers),
    consumers_ios =  VALUES(consumers_ios),
    consumers_android =  VALUES(consumers_android),
    consumers_jsj = VALUES(consumers_jsj),
    consumers_ios_jsj =  VALUES(consumers_ios_jsj),
    consumers_android_jsj =  VALUES(consumers_android_jsj),
    consumers_jsz = VALUES(consumers_jsz),
    consumers_ios_jsz =  VALUES(consumers_ios_jsz),
    consumers_android_jsz =  VALUES(consumers_android_jsz)
"""

car_consumers_jz_weekly = """
select A.s_day,
C.consumers, C.consumers_ios,C.consumers_android,
A.consumers_jsj, A.consumers_ios_jsj, A.consumers_android_jsj,
if(isnull(B.consumers_jsz), 0, B.consumers_jsz) consumers_jsz,
if(isnull(B.consumers_ios_jsz), 0, B.consumers_ios_jsz) consumers_ios_jsz,
if(isnull(B.consumers_android_jsz), 0, B.consumers_android_jsz) consumers_android_jsz
FROM
(SELECT %s s_day,
COUNT(distinct phone_id) consumers,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android
from orders
where createtime>=%s
and createtime<%s
and order_status not in (0,10)
and get_type in (1,2,5,6)) C
left join (
SELECT %s s_day,
COUNT(distinct phone_id) consumers_jsj,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_jsj,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_jsj
from orders
where createtime>=%s
and createtime<%s
and order_status not in (0,10)
and get_type in (1,2)) A on C.s_day=A.s_day
left join (
SELECT %s s_day,
COUNT(distinct phone_id) consumers_jsz,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_jsz,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_jsz
from orders
where createtime>=%s
and createtime<%s
and order_status not in (0,10)
and get_type in (5,6)) B on A.s_day = B.s_day
"""

update_car_consumers_jz_weekly = """
    insert into huoli_car_consumers_weekly values (%s, %s ,%s,%s, %s ,%s, %s,%s, %s ,%s,  now(), now())
    on duplicate key update updatetime = now(),
    consumers = VALUES(consumers),
    consumers_ios =  VALUES(consumers_ios),
    consumers_android =  VALUES(consumers_android),
    consumers_jsj = VALUES(consumers_jsj),
    consumers_ios_jsj =  VALUES(consumers_ios_jsj),
    consumers_android_jsj =  VALUES(consumers_android_jsj),
    consumers_jsz = VALUES(consumers_jsz),
    consumers_ios_jsz =  VALUES(consumers_ios_jsz),
    consumers_android_jsz =  VALUES(consumers_android_jsz)
"""

car_consumers_jz_monthly = """
select A.s_day,
C.consumers, C.consumers_ios,C.consumers_android,
A.consumers_jsj, A.consumers_ios_jsj, A.consumers_android_jsj,
if(isnull(B.consumers_jsz), 0, B.consumers_jsz) consumers_jsz,
if(isnull(B.consumers_ios_jsz), 0, B.consumers_ios_jsz) consumers_ios_jsz,
if(isnull(B.consumers_android_jsz), 0, B.consumers_android_jsz) consumers_android_jsz
FROM
(SELECT %s s_day,
COUNT(distinct phone_id) consumers,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android
from orders
where createtime>=%s
and createtime<%s
and order_status not in (0,10)
and get_type in (1,2,5,6)) C
left join (
SELECT %s s_day,
COUNT(distinct phone_id) consumers_jsj,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_jsj,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_jsj
from orders
where createtime>=%s
and createtime<%s
and order_status not in (0,10)
and get_type in (1,2)) A on C.s_day=A.s_day
left join (
SELECT %s s_day,
COUNT(distinct phone_id) consumers_jsz,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_jsz,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_jsz
from orders
where createtime>=%s
and createtime<%s
and order_status not in (0,10)
and get_type in (5,6)) B on A.s_day = B.s_day
"""

update_car_consumers_jz_monthly = """
    insert into huoli_car_consumers_monthly values (%s, %s ,%s,%s, %s ,%s, %s,%s, %s ,%s,  now(), now())
    on duplicate key update updatetime = now(),
    consumers = VALUES(consumers),
    consumers_ios =  VALUES(consumers_ios),
    consumers_android =  VALUES(consumers_android),
    consumers_jsj = VALUES(consumers_jsj),
    consumers_ios_jsj =  VALUES(consumers_ios_jsj),
    consumers_android_jsj =  VALUES(consumers_android_jsj),
    consumers_jsz = VALUES(consumers_jsz),
    consumers_ios_jsz =  VALUES(consumers_ios_jsz),
    consumers_android_jsz =  VALUES(consumers_android_jsz)
"""


car_consumers_jz_quarterly = """
select A.s_day,
C.consumers, C.consumers_ios,C.consumers_android,
A.consumers_jsj, A.consumers_ios_jsj, A.consumers_android_jsj,
if(isnull(B.consumers_jsz), 0, B.consumers_jsz) consumers_jsz,
if(isnull(B.consumers_ios_jsz), 0, B.consumers_ios_jsz) consumers_ios_jsz,
if(isnull(B.consumers_android_jsz), 0, B.consumers_android_jsz) consumers_android_jsz
FROM
(SELECT CONCAT(YEAR(%s),',','Q',QUARTER(%s)) s_day,
COUNT(distinct phone_id) consumers,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android
from orders
where createtime>=%s
and createtime<%s
and order_status not in (0,10)
and get_type in (1,2,5,6)) C
left join (
SELECT CONCAT(YEAR(%s),',','Q',QUARTER(%s)) s_day,
COUNT(distinct phone_id) consumers_jsj,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_jsj,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_jsj
from orders
where createtime>=%s
and createtime<%s
and order_status not in (0,10)
and get_type in (1,2)) A on C.s_day=A.s_day
left join (
SELECT CONCAT(YEAR(%s),',','Q',QUARTER(%s)) s_day,
COUNT(distinct phone_id) consumers_jsz,
COUNT(distinct case when paramp like '%%ios%%' then phone_id else null end) consumers_ios_jsz,
COUNT(distinct case when paramp like '%%android%%' then phone_id else null end) consumers_android_jsz
from orders
where createtime>=%s
and createtime<%s
and order_status not in (0,10)
and get_type in (5,6)) B on A.s_day = B.s_day
"""

update_car_consumers_jz_quarterly = """
    insert into huoli_car_consumers_quarterly values (%s, %s ,%s,%s, %s ,%s, %s,%s, %s ,%s,  now(), now())
    on duplicate key update updatetime = now(),
    consumers = VALUES(consumers),
    consumers_ios =  VALUES(consumers_ios),
    consumers_android =  VALUES(consumers_android),
    consumers_jsj = VALUES(consumers_jsj),
    consumers_ios_jsj =  VALUES(consumers_ios_jsj),
    consumers_android_jsj =  VALUES(consumers_android_jsj),
    consumers_jsz = VALUES(consumers_jsz),
    consumers_ios_jsz =  VALUES(consumers_ios_jsz),
    consumers_android_jsz =  VALUES(consumers_android_jsz)
"""