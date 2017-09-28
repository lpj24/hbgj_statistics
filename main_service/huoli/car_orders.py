# -*- coding: utf-8 -*-
from sql.huoli_sqlHandlers import car_orders_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import sys
reload(sys)
sys.setdefaultencoding('utf8')


def update_car_orders_daily(days=0):
    """更新伙力专车的日订单, huoli_car_orders_daily"""

    today = DateUtil.get_date_before_days(int(days))
    tomorrow = DateUtil.get_date_after_days(1-int(days))
    dto = []
    #jiesongji jisongzhan
    for i in xrange(2):
        dto.append(DateUtil.date2str(today, '%Y-%m-%d'))
        dto.append(DateUtil.date2str(today))
        dto.append(DateUtil.date2str(tomorrow))
    query_data = DBCli().car_cli.query_one(car_orders_sql["car_orders_jz_daily"], dto)
    DBCli().targetdb_cli.insert(car_orders_sql["update_car_orders_jz_daily"], query_data)


def update_gt_car():
    open_car_gt_sql = """
        SELECT t2.city_name AS 城市,
               GROUP_CONCAT(left(depot_name, POSITION('站' IN depot_name) - 1)),
               GROUP_CONCAT(t1.depot_code) AS 三字码

        FROM air_railway_depot t1
        LEFT JOIN cities t2
        ON t1.city_id = t2.city_id
        WHERE
        1=1
        and  t1.depot_type = 1
        and stat_flag=0
        GROUP BY t2.city_name;
    """

    sql = """
        select
        account_gtgj.hbgj_phoneid,
        account_gtgj.gt_user_name,
        CONCAT_WS(' ', depart_date, depart_time),
        CONCAT_WS(' ', depart_date, arrive_time),
        us.depart_name, us.arrive_name
        from user_sub_order us
        left JOIN account_gtgj on us.userid = account_gtgj.userid
        where CONCAT_WS(' ', depart_date, depart_time)>='2017-09-15 9:00'
        and CONCAT_WS(' ', depart_date, depart_time) <= '2017-09-18 23:59'
        and us.status not in ('取消订单','取消改签')
        and (arrive_name in %s
        or depart_name in %s)
    """

    gt_dict_sql = """
        SELECT
        left(depot_name, POSITION('站' IN depot_name) - 1),
        t1.depot_code

        FROM air_railway_depot t1
        LEFT JOIN cities t2
        ON t1.city_id = t2.city_id
        WHERE
        1=1
        and  t1.depot_type = 1
        and stat_flag=0;
    """
    gt_city = DBCli().car_cli.query_all(open_car_gt_sql)
    import itertools
    gt_list = list(itertools.chain(*[g[1].split(',') for g in gt_city]))
    gt_dict_data = DBCli().car_cli.query_all(gt_dict_sql)
    new_gt_dict = dict(gt_dict_data)

    gt_data = DBCli().gt_cli.query_all(sql, [gt_list]*2)
    result = []
    for gt in gt_data:
        phoneid, phone, departtime, arrtime, depname, arrname = gt
        depcode = new_gt_dict.get(depname, '')
        arrcode = new_gt_dict.get(arrname, '')
        if not phoneid:
            phoneid = ""
        if not phone:
            phone = ""
        result.append([phoneid, phone, departtime, arrtime, depname, depcode, arrname, arrcode, "\n"])

    f = open('gt.dat', 'a')
    print len(result)
    for t in result:
        out_str = "\t".join(t)
        f.write(out_str)
    f.close()

if __name__ == "__main__":
    update_gt_car()