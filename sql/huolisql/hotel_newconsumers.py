# -*- coding: utf-8 -*-

# hotel_newconsumers_daily = """
# SELECT %s s_day, count(DISTINCT phoneid) new_consumers
# from hotelorder ht
# where createtime>=%s
# and createtime<%s
# and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
# and not EXISTS (
# select distinct phoneid from hotelorder where createtime<%s
# and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
# and phoneid = ht.phoneid
# )
# """

hotel_newconsumers_daily = """
SELECT DISTINCT phoneid, 0 newconsumers_p2p
from hotelorder ht
where createtime>=%s
and createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
and not EXISTS (
select distinct phoneid from hotelorder where createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
and phoneid = ht.phoneid
)
"""

hotel_newconsumers_p2p_daily = """
SELECT count(DISTINCT phoneid)
from hotelorder ht
where createtime>=%s
and createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
and not EXISTS (
select distinct phoneid from hotelorder where createtime<%s
and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null)
and phoneid = ht.phoneid
and gdsname='p2p'
)
and gdsname='p2p'

"""

update_hotel_newconsumers_daily = """
    insert into hotel_newconsumers_daily values (%s, %s, %s, now(), now())
    on duplicate key update updatetime = now() ,
    s_day = VALUES(s_day),
    new_consumers = VALUES(new_consumers),
    new_consumers_p2p = VALUES(new_consumers_p2p)
"""