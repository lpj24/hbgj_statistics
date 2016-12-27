car_orders_daily_history = """
SELECT A.s_day,
    A.orders_num_jj, A.orders_num_ios_jj, A.orders_num_android_jj,
B.orders_num_sj, B.orders_num_ios_sj, B.orders_num_android_sj,
if(isnull(C.orders_num_jz), 0, C.orders_num_jz) orders_num_jz,
if(isnull(C.orders_num_ios_jz), 0, C.orders_num_ios_jz) orders_num_ios_jz,
if(isnull(C.orders_num_android_jz), 0, C.orders_num_android_jz) orders_num_android_jz,
if(isnull(D.orders_num_sz), 0, D.orders_num_sz) orders_num_sz,
if(isnull(D.orders_num_ios_sz), 0, D.orders_num_ios_sz) orders_num_ios_sz,
if(isnull(D.orders_num_android_sz), 0, D.orders_num_android_sz) orders_num_android_sz
FROM (
SELECT DATE_FORMAT(createtime,'%%Y-%%m-%%d') s_day,
COUNT(distinct order_id) orders_num_jj,
COUNT(distinct case when paramp like '%%ios%%' then order_id else null end) orders_num_ios_jj,
COUNT(distinct case when paramp like '%%android%%' then order_id else null end) orders_num_android_jj
from orders
where createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=1
GROUP BY s_day) A
left join (
SELECT DATE_FORMAT(createtime,'%%Y-%%m-%%d') s_day,
COUNT(distinct order_id) orders_num_sj,
COUNT(distinct case when paramp like '%%ios%%' then order_id else null end) orders_num_ios_sj,
COUNT(distinct case when paramp like '%%android%%' then order_id else null end) orders_num_android_sj
from orders
where createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=2
GROUP BY s_day) B on A.s_day = B.s_day
LEFT JOIN
(
SELECT DATE_FORMAT(createtime,'%%Y-%%m-%%d') s_day,
COUNT(distinct order_id) orders_num_jz,
COUNT(distinct case when paramp like '%%ios%%' then order_id else null end) orders_num_ios_jz,
COUNT(distinct case when paramp like '%%android%%' then order_id else null end) orders_num_android_jz
from orders
where createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=5
GROUP BY s_day) C on B.s_day = C.s_day
  LEFT JOIN
(
SELECT DATE_FORMAT(createtime,'%%Y-%%m-%%d') s_day,
COUNT(distinct order_id) orders_num_sz,
COUNT(distinct case when paramp like '%%ios%%' then order_id else null end) orders_num_ios_sz,
COUNT(distinct case when paramp like '%%android%%' then order_id else null end) orders_num_android_sz
from orders
where createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=6
GROUP BY s_day) D on C.s_day = D.s_day
"""

insert_car_orders_daily_history = """
    insert into huoli_car_orders_daily_new values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,now(), now())
"""

car_consumers_daily_history = """
select A.s_day,
A.consumers_jj, A.consumers_ios_jj, A.consumers_android_jj,B.consumers_sj, B.consumers_ios_sj, B.consumers_android_sj,
if(isnull(C.consumers_jz), 0, C.consumers_jz) consumers_jz,
if(isnull(C.consumers_ios_jz), 0, C.consumers_ios_jz) consumers_ios_jz,
if(isnull(C.consumers_android_jz), 0, C.consumers_android_jz) consumers_android_jz,
if(isnull(D.consumers_sz), 0, D.consumers_sz) consumers_sz,
if(isnull(D.consumers_ios_sz), 0, D.consumers_ios_sz) consumers_ios_sz,
if(isnull(D.consumers_android_sz), 0, D.consumers_android_sz) consumers_android_sz
from (
SELECT DATE_FORMAT(createtime,'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers_jj,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_jj,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_jj
from orders
where createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=1
GROUP BY s_day) A
LEFT JOIN
(SELECT DATE_FORMAT(createtime,'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers_sj,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_sj,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_sj
from orders
where createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=2
GROUP BY s_day) B on A.s_day=B.s_day
LEFT JOIN
(SELECT DATE_FORMAT(createtime,'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers_jz,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_jz,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_jz
from orders
where createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=5
GROUP BY s_day) C on B.s_day=C.s_day
LEFT JOIN
(SELECT DATE_FORMAT(createtime,'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers_sz,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_sz,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_sz
from orders
where createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=6
GROUP BY s_day) D on C.s_day=D.s_day
"""

insert_car_consumers_daily_history = """
    insert into huoli_car_consumers_daily values (%s, %s ,%s, %s,%s, %s ,%s, %s,%s, %s ,%s, %s,%s,  now(), now())
"""

car_newconsumers_daily_history = """
    select A.s_day,
    C.new_consumers, C.new_consumers_ios, C.new_consumers_android, C.new_orders, C.new_orders_ios, C.new_orders_android,
    A.new_consumers_jsj, A.new_consumers_ios_jsj, A.new_consumers_android_jsj, A.new_orders_jsj, A.new_orders_ios_jsj, A.new_orders_android_jsj,
    B.new_consumers_jsz, B.new_consumers_ios_jsz, B.new_consumers_android_jsz, B.new_orders_jsz, B.new_orders_ios_jsz, B.new_orders_android_jsz
    from (    SELECT %s s_day,
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
    when createtime<"2014-09-26 00:00:00" then (get_type is null or get_type in (5,6)) end))) C
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
     ) A on C.s_day = A.s_day
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

insert_car_newconsumers_daily_history = """
    insert into huoli_car_newconsumers_daily values (%s, %s, %s, %s, %s,%s,%s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s, now(), now())
"""


car_consumers_weekly_history = """
select A.s_day,
A.consumers_jj, A.consumers_ios_jj, A.consumers_android_jj,B.consumers_sj, B.consumers_ios_sj, B.consumers_android_sj,
if(isnull(C.consumers_jz), 0, C.consumers_jz) consumers_jz,
if(isnull(C.consumers_ios_jz), 0, C.consumers_ios_jz) consumers_ios_jz,
if(isnull(C.consumers_android_jz), 0, C.consumers_android_jz) consumers_android_jz,
if(isnull(D.consumers_sz), 0, D.consumers_sz) consumers_sz,
if(isnull(D.consumers_ios_sz), 0, D.consumers_ios_sz) consumers_ios_sz,
if(isnull(D.consumers_android_sz), 0, D.consumers_android_sz) consumers_android_sz
from (
SELECT date_format(subdate(createtime,date_format(createtime,'%%w')-1),'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers_jj,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_jj,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_jj
from orders
where createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=1
GROUP BY s_day) A
LEFT JOIN
(SELECT date_format(subdate(createtime,date_format(createtime,'%%w')-1),'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers_sj,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_sj,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_sj
from orders
where createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=2
GROUP BY s_day) B on A.s_day=B.s_day
LEFT JOIN
(SELECT date_format(subdate(createtime,date_format(createtime,'%%w')-1),'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers_jz,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_jz,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_jz
from orders
where createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=5
GROUP BY s_day) C on B.s_day=C.s_day
LEFT JOIN
(SELECT date_format(subdate(createtime,date_format(createtime,'%%w')-1),'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers_sz,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_sz,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_sz
from orders
where createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=6
GROUP BY s_day) D on C.s_day=D.s_day
"""

insert_car_consumers_weekly_history = """
    insert into huoli_car_consumers_weekly values (%s, %s ,%s, %s,%s, %s ,%s, %s,%s, %s ,%s, %s,%s,  now(), now())
"""

car_consumers_monthly_history = """
select A.s_day,
A.consumers_jj, A.consumers_ios_jj, A.consumers_android_jj,B.consumers_sj, B.consumers_ios_sj, B.consumers_android_sj,
if(isnull(C.consumers_jz), 0, C.consumers_jz) consumers_jz,
if(isnull(C.consumers_ios_jz), 0, C.consumers_ios_jz) consumers_ios_jz,
if(isnull(C.consumers_android_jz), 0, C.consumers_android_jz) consumers_android_jz,
if(isnull(D.consumers_sz), 0, D.consumers_sz) consumers_sz,
if(isnull(D.consumers_ios_sz), 0, D.consumers_ios_sz) consumers_ios_sz,
if(isnull(D.consumers_android_sz), 0, D.consumers_android_sz) consumers_android_sz
from (
SELECT DATE_FORMAT(str_to_date(CONCAT(YEAR(createtime),'-',MONTH(createtime),'-01'),'%%Y-%%m-%%d'),'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers_jj,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_jj,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_jj
from orders
where createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=1
GROUP BY s_day) A
LEFT JOIN
(SELECT DATE_FORMAT(str_to_date(CONCAT(YEAR(createtime),'-',MONTH(createtime),'-01'),'%%Y-%%m-%%d'),'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers_sj,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_sj,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_sj
from orders
where createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=2
GROUP BY s_day) B on A.s_day=B.s_day
LEFT JOIN
(SELECT DATE_FORMAT(str_to_date(CONCAT(YEAR(createtime),'-',MONTH(createtime),'-01'),'%%Y-%%m-%%d'),'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers_jz,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_jz,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_jz
from orders
where createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=5
GROUP BY s_day) C on B.s_day=C.s_day
LEFT JOIN
(SELECT DATE_FORMAT(str_to_date(CONCAT(YEAR(createtime),'-',MONTH(createtime),'-01'),'%%Y-%%m-%%d'),'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers_sz,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_sz,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_sz
from orders
where createtime<%s
and order_status in (11,13,14,18,31,32)
and get_type=6
GROUP BY s_day) D on C.s_day=D.s_day
"""

insert_car_consumers_monthly_history = """
    insert into huoli_car_consumers_monthly values (%s, %s ,%s, %s,%s, %s ,%s, %s,%s, %s ,%s, %s,%s,  now(), now())
"""


car_consumers_quarterly_history = """
select A.s_day,
A.consumers_jj, A.consumers_ios_jj, A.consumers_android_jj,B.consumers_sj, B.consumers_ios_sj, B.consumers_android_sj,
if(isnull(C.consumers_jz), 0, C.consumers_jz) consumers_jz,
if(isnull(C.consumers_ios_jz), 0, C.consumers_ios_jz) consumers_ios_jz,
if(isnull(C.consumers_android_jz), 0, C.consumers_android_jz) consumers_android_jz,
if(isnull(D.consumers_sz), 0, D.consumers_sz) consumers_sz,
if(isnull(D.consumers_ios_sz), 0, D.consumers_ios_sz) consumers_ios_sz,
if(isnull(D.consumers_android_sz), 0, D.consumers_android_sz) consumers_android_sz
from (
SELECT CONCAT(YEAR(createtime),',','Q',QUARTER(createtime)) s_day,
COUNT(distinct phone_id) consumers_jj,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_jj,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_jj
from orders
where createtime<'2016-04-01 00:00:00'
and order_status in (11,13,14,18,31,32)
and get_type=1
GROUP BY s_day) A
LEFT JOIN
(SELECT CONCAT(YEAR(createtime),',','Q',QUARTER(createtime)) s_day,
COUNT(distinct phone_id) consumers_sj,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_sj,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_sj
from orders
where createtime<'2016-04-01 00:00:00'
and order_status in (11,13,14,18,31,32)
and get_type=2
GROUP BY s_day) B on A.s_day=B.s_day
LEFT JOIN
(SELECT CONCAT(YEAR(createtime),',','Q',QUARTER(createtime)) s_day,
COUNT(distinct phone_id) consumers_jz,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_jz,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_jz
from orders
where createtime<'2016-04-01 00:00:00'
and order_status in (11,13,14,18,31,32)
and get_type=5
GROUP BY s_day) C on B.s_day=C.s_day
LEFT JOIN
(SELECT CONCAT(YEAR(createtime),',','Q',QUARTER(createtime)) s_day,
COUNT(distinct phone_id) consumers_sz,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_sz,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_sz
from orders
where createtime<'2016-04-01 00:00:00'
and order_status in (11,13,14,18,31,32)
and get_type=6
GROUP BY s_day) D on C.s_day=D.s_day
"""

insert_car_consumers_quarterly_history = """
    insert into huoli_car_consumers_quarterly values (%s, %s ,%s, %s,%s, %s ,%s, %s,%s, %s ,%s, %s,%s,  now(), now())
"""


car_orders_jz_daily_history = """
SELECT A.s_day,
A.orders_num_jsj, A.orders_num_ios_jsj, A.orders_num_android_jsj,
if(isnull(B.orders_num_jsz), 0, B.orders_num_jsz) orders_num_jsz,
if(isnull(B.orders_num_ios_jsz), 0, B.orders_num_ios_jsz) orders_num_ios_jsz,
if(isnull(B.orders_num_android_jsz), 0, B.orders_num_android_jsz) orders_num_android_jsz
FROM (
SELECT DATE_FORMAT(createtime,'%%Y-%%m-%%d') s_day,
COUNT(distinct order_id) orders_num_jsj,
COUNT(distinct case when paramp like '%%ios%%' then order_id else null end) orders_num_ios_jsj,
COUNT(distinct case when paramp like '%%android%%' then order_id else null end) orders_num_android_jsj
from orders
where createtime<%s
and order_status not in (0,10)
and (
    case when createtime>="2014-09-26 00:00:00" then (get_type in (1,2))
    when createtime<"2014-09-26 00:00:00" then (get_type is null) end)
GROUP BY s_day) A
left join (
SELECT DATE_FORMAT(createtime,'%%Y-%%m-%%d') s_day,
COUNT(distinct order_id) orders_num_jsz,
COUNT(distinct case when paramp like '%%ios%%' then order_id else null end) orders_num_ios_jsz,
COUNT(distinct case when paramp like '%%android%%' then order_id else null end) orders_num_android_jsz
from orders
where createtime<%s
and order_status not in (0,10)
and get_type in (5,6)
GROUP BY s_day) B on A.s_day = B.s_day
"""

insert_car_orders_jz_daily_history = """
    insert into huoli_car_orders_daily values (%s, %s, %s, %s, %s, %s, %s,now(), now())
"""


car_consumers_jz_daily_history = """
select A.s_day,
C.consumers, C.consumers_ios,C.consumers_android,
A.consumers_jsj, A.consumers_ios_jsj, A.consumers_android_jsj,
if(isnull(B.consumers_jsz), 0, B.consumers_jsz) consumers_jsz,
if(isnull(B.consumers_ios_jsz), 0, B.consumers_ios_jsz) consumers_ios_jsz,
if(isnull(B.consumers_android_jsz), 0, B.consumers_android_jsz) consumers_android_jsz
from (
SELECT DATE_FORMAT(createtime,'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android
from orders
where createtime<%s
and order_status not in (0,10)
and (
    case when createtime>="2014-09-26 00:00:00" then (get_type in (1,2,5,6))
    when createtime<"2014-09-26 00:00:00" then (get_type is null or get_type in (5,6)) end)
GROUP BY s_day
) C
left join(
SELECT DATE_FORMAT(createtime,'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers_jsj,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_jsj,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_jsj
from orders
where createtime<%s
and order_status not in (0,10)
and (
    case when createtime>="2014-09-26 00:00:00" then (get_type in (1,2))
    when createtime<"2014-09-26 00:00:00" then (get_type is null) end)
GROUP BY s_day) A on C.s_day=A.s_day
LEFT JOIN
(SELECT DATE_FORMAT(createtime,'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers_jsz,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_jsz,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_jsz
from orders
where createtime<%s
and order_status not in (0,10)
and get_type in (5,6)
GROUP BY s_day) B on A.s_day=B.s_day
"""

insert_car_consumers_jz_daily_history = """
    insert into huoli_car_consumers_daily values (%s, %s ,%s,%s, %s ,%s, %s,%s, %s ,%s,  now(), now())
"""

car_consumers_jz_weekly_history = """
select A.s_day,
C.consumers, C.consumers_ios,C.consumers_android,
A.consumers_jsj, A.consumers_ios_jsj, A.consumers_android_jsj,
if(isnull(B.consumers_jsz), 0, B.consumers_jsz) consumers_jsz,
if(isnull(B.consumers_ios_jsz), 0, B.consumers_ios_jsz) consumers_ios_jsz,
if(isnull(B.consumers_android_jsz), 0, B.consumers_android_jsz) consumers_android_jsz
from (
SELECT date_format(subdate(createtime,date_format(createtime,'%%w')-1),'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android
from orders
where createtime<%s
and order_status not in (0,10)
and (
    case when createtime>="2014-09-26 00:00:00" then (get_type in (1,2,5,6))
    when createtime<"2014-09-26 00:00:00" then (get_type is null or get_type in (5,6)) end)
GROUP BY s_day
) C
left join(
SELECT date_format(subdate(createtime,date_format(createtime,'%%w')-1),'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers_jsj,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_jsj,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_jsj
from orders
where createtime<%s
and order_status not in (0,10)
and (
    case when createtime>="2014-09-26 00:00:00" then (get_type in (1,2))
    when createtime<"2014-09-26 00:00:00" then (get_type is null) end)
GROUP BY s_day) A on C.s_day=A.s_day
LEFT JOIN
(SELECT date_format(subdate(createtime,date_format(createtime,'%%w')-1),'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers_jsz,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_jsz,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_jsz
from orders
where createtime<%s
and order_status not in (0,10)
and get_type in (5,6)
GROUP BY s_day) B on A.s_day=B.s_day
"""

insert_car_consumers_jz_weekly_history = """
    insert into huoli_car_consumers_weekly values (%s, %s ,%s,%s, %s ,%s, %s,%s, %s ,%s,  now(), now())
"""


car_consumers_jz_monthly_history = """
select A.s_day,
C.consumers, C.consumers_ios,C.consumers_android,
A.consumers_jsj, A.consumers_ios_jsj, A.consumers_android_jsj,
if(isnull(B.consumers_jsz), 0, B.consumers_jsz) consumers_jsz,
if(isnull(B.consumers_ios_jsz), 0, B.consumers_ios_jsz) consumers_ios_jsz,
if(isnull(B.consumers_android_jsz), 0, B.consumers_android_jsz) consumers_android_jsz
from (
SELECT DATE_FORMAT(str_to_date(CONCAT(YEAR(createtime),'-',MONTH(createtime),'-01'),'%%Y-%%m-%%d'),'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android
from orders
where createtime<%s
and order_status not in (0,10)
and (
    case when createtime>="2014-09-26 00:00:00" then (get_type in (1,2,5,6))
    when createtime<"2014-09-26 00:00:00" then (get_type is null or get_type in (5,6)) end)
GROUP BY s_day
) C
left join(
SELECT DATE_FORMAT(str_to_date(CONCAT(YEAR(createtime),'-',MONTH(createtime),'-01'),'%%Y-%%m-%%d'),'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers_jsj,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_jsj,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_jsj
from orders
where createtime<%s
and order_status not in (0,10)
and (
    case when createtime>="2014-09-26 00:00:00" then (get_type in (1,2))
    when createtime<"2014-09-26 00:00:00" then (get_type is null) end)
GROUP BY s_day) A on C.s_day=A.s_day
LEFT JOIN
(SELECT DATE_FORMAT(str_to_date(CONCAT(YEAR(createtime),'-',MONTH(createtime),'-01'),'%%Y-%%m-%%d'),'%%Y-%%m-%%d') s_day,
COUNT(distinct phone_id) consumers_jsz,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_jsz,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_jsz
from orders
where createtime<%s
and order_status not in (0,10)
and get_type in (5,6)
GROUP BY s_day) B on A.s_day=B.s_day
"""

insert_car_consumers_jz_monthly_history = """
    insert into huoli_car_consumers_monthly values (%s, %s ,%s,%s, %s ,%s, %s,%s, %s ,%s,  now(), now())
"""

car_consumers_jz_quarterly_history = """
select A.s_day,
C.consumers, C.consumers_ios,C.consumers_android,
A.consumers_jsj, A.consumers_ios_jsj, A.consumers_android_jsj,
if(isnull(B.consumers_jsz), 0, B.consumers_jsz) consumers_jsz,
if(isnull(B.consumers_ios_jsz), 0, B.consumers_ios_jsz) consumers_ios_jsz,
if(isnull(B.consumers_android_jsz), 0, B.consumers_android_jsz) consumers_android_jsz
from (
SELECT CONCAT(YEAR(createtime),',','Q',QUARTER(createtime)) s_day,
COUNT(distinct phone_id) consumers,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android
from orders
where createtime<'2016-04-01 00:00:00'
and order_status not in (0,10)
and (
    case when createtime>="2014-09-26 00:00:00" then (get_type in (1,2,5,6))
    when createtime<"2014-09-26 00:00:00" then (get_type is null or get_type in (5,6)) end)
GROUP BY s_day) C
left join(
SELECT CONCAT(YEAR(createtime),',','Q',QUARTER(createtime)) s_day,
COUNT(distinct phone_id) consumers_jsj,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_jsj,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_jsj
from orders
where createtime<'2016-04-01 00:00:00'
and order_status not in (0,10)
and (
    case when createtime>="2014-09-26 00:00:00" then (get_type in (1,2))
    when createtime<"2014-09-26 00:00:00" then (get_type is null) end)
GROUP BY s_day) A on C.s_day = A.s_day
LEFT JOIN
(SELECT CONCAT(YEAR(createtime),',','Q',QUARTER(createtime)) s_day,
COUNT(distinct phone_id) consumers_jsz,
COUNT(distinct (case when paramp like '%%ios%%' then phone_id else null end)) consumers_ios_jsz,
COUNT(distinct (case when paramp like '%%android%%' then phone_id else null end)) consumers_android_jsz
from orders
where createtime<'2016-04-01 00:00:00'
and order_status not in (0,10)
and get_type in (5,6)
GROUP BY s_day) B on A.s_day=B.s_day
"""

insert_car_consumers_jz_quarterly_history = """
   insert into huoli_car_consumers_quarterly values (%s, %s ,%s,%s, %s ,%s, %s,%s, %s ,%s,  now(), now())
"""