# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from sql.hb_sqlHandlers import hb_orders_date_sql


def update_hb_gt_order_daily(days=0):
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(int(days) * 3), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)), '%Y-%m-%d')
    dto = [start_date, end_date]
    query_data = DBCli().sourcedb_cli.queryAll(hb_orders_date_sql["hb_gt_order_daily_sql"], dto)
    DBCli().targetdb_cli.batchInsert(hb_orders_date_sql["update_hb_gt_order_daily_sql"], query_data)


def update_hb_gt_order_daily_his():
    import datetime
    start_date = datetime.date(2012, 11, 22)
    end_date = datetime.date(2017, 2, 6)
    dto = [start_date, end_date]
    query_data = DBCli().sourcedb_cli.queryAll(hb_orders_date_sql["hb_gt_order_daily_sql"], dto)
    DBCli().targetdb_cli.batchInsert(hb_orders_date_sql["update_hb_gt_order_daily_sql"], query_data)


def update_operation_hbgj_order_detail_daily(days=0):
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(int(days) * 1), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)), '%Y-%m-%d')
    dto = [start_date, end_date]
    sql = """
        SELECT DATE_FORMAT(od.createtime, '%%Y-%%m-%%d') s_day,
        sum(case when o.P like '%%hbgj%%' and o.INTFLAG=0 then 1 else 0 END) ticket_hbgj_inland,
        sum(case when o.P like '%%hbgj%%' and o.INTFLAG=1 then 1 else 0 END) ticket_hbgj_inter,
        sum(case when o.P like '%%gtgj%%' and o.INTFLAG=0 then 1 else 0 END) ticket_gtgj_inland,
        sum(case when o.P like '%%gtgj%%' and o.INTFLAG=1 then 1 else 0 END) ticket_gtgj_inter,
        sum(case when o.INTFLAG=0 then 1 else 0 END) ticket_inland,
        sum(case when o.INTFLAG=1 then 1 else 0 END) ticket_inter,
        count(DISTINCT case when o.P like '%%hbgj%%' and o.INTFLAG=0 then o.ORDERID END) order_hbgj_inland,
        count(DISTINCT case when o.P like '%%hbgj%%' and o.INTFLAG=1 then o.ORDERID END) order_hbgj_inter,
        count(DISTINCT case when o.P like '%%gtgj%%' and o.INTFLAG=0 then o.ORDERID END) order_gtgj_inland,
        count(DISTINCT case when o.P like '%%gtgj%%' and o.INTFLAG=1 then o.ORDERID END) order_gtgj_inter,
        count(DISTINCT case when o.INTFLAG=0 then o.ORDERID END) order_inland,
        count(DISTINCT case when o.INTFLAG=1 then o.ORDERID END) order_inter,
        sum(case when o.P like '%%hbgj%%' and o.INTFLAG=0 then o.PAYPRICE else 0 END) gmv_hbgj_inland,
        sum(case when o.P like '%%hbgj%%' and o.INTFLAG=1 then o.PAYPRICE else 0 END) gmv_hbgj_inter,
        sum(case when o.P like '%%gtgj%%' and o.INTFLAG=0 then o.PAYPRICE else 0 END) gmv_gtgj_inland,
        sum(case when o.P like '%%gtgj%%' and o.INTFLAG=1 then o.PAYPRICE else 0 END) gmv_gtgj_inter,
        sum(case when o.INTFLAG=0 then o.PAYPRICE else 0 END) gmv_inland,
        sum(case when o.INTFLAG=1 then o.PAYPRICE else 0 END) gmv_inter
        FROM `TICKET_ORDERDETAIL` od
        INNER JOIN `TICKET_ORDER` o
        on od.ORDERID=o.ORDERID
        where od.CREATETIME>=%s
        and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        AND IFNULL(od.`LINKTYPE`, 0) != 2
        GROUP BY s_day;

    """

    insert_sql = """
        insert into operation_hbgj_order_detail_daily (s_day, ticket_hbgj_inland, ticket_hbgj_inter, ticket_gtgj_inland,
            ticket_gtgj_inter, ticket_inland, ticket_inter, order_hbgj_inland, order_hbgj_inter, order_gtgj_inland,
            order_gtgj_inter, order_inland, order_inter, gmv_hbgj_inland,
            gmv_hbgj_inter, gmv_gtgj_inland, gmv_gtgj_inter, gmv_inland, gmv_inter, createtime, updatetime
        ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        ticket_hbgj_inland = values(ticket_hbgj_inland),
        ticket_hbgj_inter = values(ticket_hbgj_inter),
        ticket_gtgj_inland = values(ticket_gtgj_inland),
        ticket_gtgj_inter = values(ticket_gtgj_inter),
        ticket_inland = values(ticket_inland),
        ticket_inter = values(ticket_inter),
        order_hbgj_inland = values(order_hbgj_inland),
        order_hbgj_inter = values(order_hbgj_inter),
        order_gtgj_inland = values(order_gtgj_inland),
        order_gtgj_inter = values(order_gtgj_inter),
        order_inland = values(order_inland),
        order_inter = values(order_inter),
        gmv_hbgj_inland = values(gmv_hbgj_inland),
        gmv_hbgj_inter = values(gmv_hbgj_inter),
        gmv_gtgj_inland = values(gmv_gtgj_inland),
        gmv_gtgj_inter = values(gmv_gtgj_inter),
        gmv_inland = values(gmv_inland),
        gmv_inter = values(gmv_inter)

    """
    query_data = DBCli().sourcedb_cli.queryOne(sql, dto)
    DBCli().targetdb_cli.insert(insert_sql, query_data)


if __name__ == "__main__":
    update_hb_gt_order_daily(1)
    update_operation_hbgj_order_detail_daily(1)
    # update_hb_gt_order_daily_his()