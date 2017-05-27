# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from sql.hb_sqlHandlers import hb_orders_date_sql


def update_hb_gt_order_daily(days=0):
    """更新航班高铁订单, hbgj_order_detail_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(int(days) * 3), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)), '%Y-%m-%d')
    dto = [start_date, end_date]
    query_data = DBCli().sourcedb_cli.queryAll(hb_orders_date_sql["hb_gt_order_daily_sql"], dto)
    DBCli().targetdb_cli.batchInsert(hb_orders_date_sql["update_hb_gt_order_daily_sql"], query_data)
    return __file__


def update_hb_gt_order_daily_his():
    import datetime
    start_date = datetime.date(2012, 11, 22)
    end_date = datetime.date(2017, 2, 6)
    dto = [start_date, end_date]
    query_data = DBCli().sourcedb_cli.queryAll(hb_orders_date_sql["hb_gt_order_daily_sql"], dto)
    DBCli().targetdb_cli.batchInsert(hb_orders_date_sql["update_hb_gt_order_daily_sql"], query_data)


def update_operation_hbgj_order_detail_daily(days=0):
    """更新航班管家订单详情, operation_hbgj_order_detail_daily"""
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
    return __file__


def update_hb_gt_order_new_daily(days=0):
    """更新航班高铁订单(new), hbgj_order_detail_daily_new"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(int(days) * 3), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)), '%Y-%m-%d')
    dto = [start_date, end_date]
    total_order_sql = """
        SELECT DATE_FORMAT(TICKET_ORDER.CREATETIME,'%%Y-%%m-%%d') s_day,
        count(distinct TICKET_ORDERDETAIL.ORDERID) order_num,
        count(TICKET_ORDERDETAIL.ORDERID) ticket_num,
        count(distinct case when TICKET_ORDER.p like '%%gtgj%%' then TICKET_ORDERDETAIL.ORDERID END) order_num_gt,
        count(case when TICKET_ORDER.p like '%%gtgj%%' then TICKET_ORDERDETAIL.ORDERID END) ticket_num_gt
        FROM TICKET_ORDERDETAIL
        join TICKET_ORDER
        ON TICKET_ORDER.ORDERID = TICKET_ORDERDETAIL.ORDERID
        where TICKET_ORDER.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        and  TICKET_ORDER.CREATETIME >= %s
        and  TICKET_ORDER.CREATETIME < %s
        AND IFNULL(TICKET_ORDERDETAIL.`LINKTYPE`, 0) != 2
        GROUP BY s_day
    """

    insert_total_sql = """
        insert into hbgj_order_detail_daily_new
        (s_day, order_num, ticket_num, order_num_gt, ticket_num_gt,
        createtime, updatetime)
        values (%s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        order_num = values(order_num),
        ticket_num = values(ticket_num),
        order_num_gt = values(order_num_gt),
        ticket_num_gt = values(ticket_num_gt)
    """
    hbgj_detail_sql = """
        SELECT
        sum(case when o.INTFLAG=0 then 1 else 0 END) ticket_inland,
        count(distinct case when o.INTFLAG=0 then o.ORDERID END) order_inland,
        sum(case when o.INTFLAG=1 then 1 else 0 END) ticket_inter,
        count(distinct case when o.INTFLAG=1 then o.ORDERID END) order_inter,
        DATE_FORMAT(od.createtime, '%%Y-%%m-%%d') s_day
        FROM `TICKET_ORDERDETAIL` od
        INNER JOIN `TICKET_ORDER` o
        on od.ORDERID=o.ORDERID
        where od.CREATETIME>=%s
        and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        AND IFNULL(od.`LINKTYPE`, 0) != 2
        GROUP BY s_day
    """

    update_sql = """
        update hbgj_order_detail_daily_new
        set inland_ticket_num=%s, inland_order_num=%s, inter_ticket_num=%s,
        inter_order_num=%s where s_day=%s
    """
    total_query_data = DBCli().sourcedb_cli.queryAll(total_order_sql, dto)
    DBCli().targetdb_cli.batchInsert (insert_total_sql, total_query_data)

    query_data = DBCli().sourcedb_cli.queryAll(hbgj_detail_sql, dto)
    DBCli().targetdb_cli.batchInsert(update_sql, query_data)
    return __file__

if __name__ == "__main__":
    # update_hb_gt_order_daily(1)
    # update_operation_hbgj_order_detail_daily(1)
    # update_hb_gt_order_daily_his()
    update_hb_gt_order_new_daily(1)