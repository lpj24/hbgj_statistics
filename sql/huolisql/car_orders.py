car_orders_daily = """
SELECT A.s_day,
    A.orders_num_jj, A.orders_num_ios_jj, A.orders_num_android_jj,
B.orders_num_sj, B.orders_num_ios_sj, B.orders_num_android_sj,
    C.orders_num_jz, C.orders_num_ios_jz, C.orders_num_android_jz,
D.orders_num_sz, D.orders_num_ios_sz, D.orders_num_android_sz
FROM (
SELECT %s s_day,
COUNT(distinct order_id) orders_num_jj,
COUNT(distinct case when paramp like '%%ios%%' then order_id else null end) orders_num_ios_jj,
COUNT(distinct case when paramp like '%%android%%' then order_id else null end) orders_num_android_jj
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=1) A
left join (
SELECT %s s_day,
COUNT(distinct order_id) orders_num_sj,
COUNT(distinct case when paramp like '%%ios%%' then order_id else null end) orders_num_ios_sj,
COUNT(distinct case when paramp like '%%android%%' then order_id else null end) orders_num_android_sj
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=2) B on A.s_day = B.s_day
LEFT JOIN
(
SELECT %s s_day,
COUNT(distinct order_id) orders_num_jz,
COUNT(distinct case when paramp like '%%ios%%' then order_id else null end) orders_num_ios_jz,
COUNT(distinct case when paramp like '%%android%%' then order_id else null end) orders_num_android_jz
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=5) C on B.s_day = C.s_day
  LEFT JOIN
(
SELECT %s s_day,
COUNT(distinct order_id) orders_num_sz,
COUNT(distinct case when paramp like '%%ios%%' then order_id else null end) orders_num_ios_sz,
COUNT(distinct case when paramp like '%%android%%' then order_id else null end) orders_num_android_sz
from orders
where createtime>=%s
and createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=6) D on C.s_day = D.s_day
"""

update_car_orders_daily = """
    insert into huoli_car_orders_daily_new values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,now(), now())
    on duplicate key update updatetime = now(),
    orders_num_jj = VALUES(orders_num_jj),
    orders_num_ios_jj =  VALUES(orders_num_ios_jj),
    orders_num_android_jj =  VALUES(orders_num_android_jj),
    orders_num_sj = VALUES(orders_num_sj),
    orders_num_ios_sj =  VALUES(orders_num_ios_sj),
    orders_num_android_sj =  VALUES(orders_num_android_sj),
    orders_num_jz = VALUES(orders_num_jz),
    orders_num_ios_jz =  VALUES(orders_num_ios_jz),
    orders_num_android_jz =  VALUES(orders_num_android_jz),
    orders_num_sz = VALUES(orders_num_sz),
    orders_num_ios_sz =  VALUES(orders_num_ios_sz),
    orders_num_android_sz =  VALUES(orders_num_android_sz)
"""

car_orders_jz_daily = """
SELECT A.s_day,
A.orders_num_jsj, A.orders_num_ios_jsj, A.orders_num_android_jsj,
if(isnull(B.orders_num_jsz), 0, B.orders_num_jsz) orders_num_jsz,
if(isnull(B.orders_num_ios_jsz), 0, B.orders_num_ios_jsz) orders_num_ios_jsz,
if(isnull(B.orders_num_android_jsz), 0, B.orders_num_android_jsz) orders_num_android_jsz
FROM (
SELECT %s s_day,
COUNT(distinct order_id) orders_num_jsj,
COUNT(distinct case when paramp like '%%ios%%' then order_id else null end) orders_num_ios_jsj,
COUNT(distinct case when paramp like '%%android%%' then order_id else null end) orders_num_android_jsj
from orders
where createtime>=%s
and createtime<%s
and order_status not in (0,10)
and get_type in (1,2)) A
left join (
SELECT %s s_day,
COUNT(distinct order_id) orders_num_jsz,
COUNT(distinct case when paramp like '%%ios%%' then order_id else null end) orders_num_ios_jsz,
COUNT(distinct case when paramp like '%%android%%' then order_id else null end) orders_num_android_jsz
from orders
where createtime>=%s
and createtime<%s
and order_status not in (0,10)
and get_type in (5,6)) B on A.s_day = B.s_day
"""

update_car_orders_jz_daily = """
    insert into huoli_car_orders_daily values (%s, %s, %s, %s, %s, %s, %s,now(), now())
    on duplicate key update updatetime = now(),
    orders_num_jsj = VALUES(orders_num_jsj),
    orders_num_ios_jsj =  VALUES(orders_num_ios_jsj),
    orders_num_android_jsj =  VALUES(orders_num_android_jsj),
    orders_num_jsz = VALUES(orders_num_jsz),
    orders_num_ios_jsz =  VALUES(orders_num_ios_jsz),
    orders_num_android_jsz =  VALUES(orders_num_android_jsz)
"""