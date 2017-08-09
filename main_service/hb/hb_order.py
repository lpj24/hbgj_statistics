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
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(int(days) * 3))
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)))
    dto = [start_date, end_date]

    insert_total_sql = """
        insert into hbgj_order_detail_daily_new
        (s_day, order_num, ticket_num, order_num_gt, ticket_num_gt,
        inland_order_num, inland_ticket_num, inter_order_num, inter_ticket_num,
        createtime, updatetime)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        order_num = values(order_num),
        ticket_num = values(ticket_num),
        order_num_gt = values(order_num_gt),
        ticket_num_gt = values(ticket_num_gt),
        inland_order_num = values(inland_order_num),
        inland_ticket_num = values(inland_ticket_num),
        inter_order_num = values(inter_order_num),
        inter_ticket_num = values(inter_ticket_num)
    """
    hbgj_detail_sql = """
        SELECT
        DATE_FORMAT(od.createtime, '%%Y-%%m-%%d') s_day,
        count(distinct od.ORDERID) order_num,
        count(od.ORDERID) ticket_num,
        count(distinct case when o.p like '%%gtgj%%' then od.ORDERID END) order_num_gt,
        count(case when o.p like '%%gtgj%%' then od.ORDERID END) ticket_num_gt,
        count(distinct case when o.INTFLAG=0 then o.ORDERID END) inland_order_num,
        sum(case when o.INTFLAG=0 then 1 else 0 END) inland_ticket_num,
        count(distinct case when o.INTFLAG=1 then o.ORDERID END) inter_order_num,
        sum(case when o.INTFLAG=1 then 1 else 0 END) inter_ticket_num
        FROM `TICKET_ORDERDETAIL` od
        INNER JOIN `TICKET_ORDER` o
        on od.ORDERID=o.ORDERID
        and  od.CREATETIME >= %s
        and  od.CREATETIME < %s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        AND IFNULL(od.`LINKTYPE`, 0) != 2
        GROUP BY s_day;
    """

    total_query_data = DBCli().sourcedb_cli.queryAll(hbgj_detail_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_total_sql, total_query_data)

    return __file__


def update_hbgj_ticket_region_inter_daily(days=0):
    """更新国际航班各地区订票统计, hbgj_ticket_region_inter_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(int(days) * 60))
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)))
    dto = [start_date, end_date]

    insert_total_sql = """
        insert into hbgj_ticket_region_inter_daily
        (s_day, total_ticket_num, gat, dny, japan, NA, EU, korea, OA,
        Middle_AISA, AF, SA, createtime, updatetime)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        total_ticket_num = values(total_ticket_num),
        gat = values(gat),
        dny = values(dny),
        japan = values(japan),
        NA = values(NA),
        EU = values(EU),
        korea = values(korea),
        OA = values(OA),
        Middle_AISA = values(Middle_AISA),
        AF = values(AF),
        SA = values(SA)
    """
    query_sql = """
        SELECT DATE_FORMAT(t.CREATETIME, '%%Y-%%m-%%d') s_day,
        sum(1) total,
        SUM(CASE WHEN
            (td.ARRCODE IN
            (SELECT THREE_WORDS_CODE FROM apibase.AIRPORT_NATION_INFO)
            and
            td.DEPCODE in (select THREE_WORDS_CODE from apibase.AIRPORT_INTER_INFO where COUNTRY in ('香港地区', '澳门地区', '中国台湾')
            ) ) or 	(td.DEPCODE IN
            (SELECT THREE_WORDS_CODE FROM apibase.AIRPORT_NATION_INFO)
            and
            td.ARRCODE in (select THREE_WORDS_CODE from apibase.AIRPORT_INTER_INFO where COUNTRY in ('香港地区', '澳门地区', '中国台湾')
            ) )
        THEN 1 ELSE 0
        END) gat,

        sum(
            EXISTS
                (
                    select THREE_WORDS_CODE from apibase.AIRPORT_INTER_INFO WHERE COUNTRY IN
                        ('越南', '泰国', '老挝', '柬埔寨', '缅甸', '马来西亚', '新加坡'
                        , '印度尼西亚', '文莱', '菲律宾', '东帝汶') and
                        (td.DEPCODE=THREE_WORDS_CODE or td.ARRCODE=THREE_WORDS_CODE)
                )
        ) dny,

        sum(
        EXISTS (
                select THREE_WORDS_CODE from apibase.AIRPORT_INTER_INFO where COUNTRY in ('日本')
                    and
                (td.DEPCODE=THREE_WORDS_CODE or td.ARRCODE=THREE_WORDS_CODE)
        )
        ) japan,

        sum(
         EXISTS
         (
            select THREE_WORDS_CODE from apibase.AIRPORT_INTER_INFO where REGION_CODE='NA'
            AND (td.DEPCODE=THREE_WORDS_CODE or td.ARRCODE=THREE_WORDS_CODE)
         )
        ) bm,

        sum(
        EXISTS
        (
          select THREE_WORDS_CODE from apibase.AIRPORT_INTER_INFO where REGION_CODE='EU'
            AND (td.DEPCODE=THREE_WORDS_CODE or td.ARRCODE=THREE_WORDS_CODE)
        )
        ) oz,


        sum(
        EXISTS (
            select THREE_WORDS_CODE from apibase.AIRPORT_INTER_INFO where COUNTRY in ('韩国')
            AND (td.DEPCODE=THREE_WORDS_CODE or td.ARRCODE=THREE_WORDS_CODE)
        )
        ) hg,

        sum(
        EXISTS
        (
            select THREE_WORDS_CODE from apibase.AIRPORT_INTER_INFO where REGION_CODE='OA'
            AND (td.DEPCODE=THREE_WORDS_CODE or td.ARRCODE=THREE_WORDS_CODE)

        )
        ) az,

        sum(
        EXISTS
        (
            select THREE_WORDS_CODE from apibase.AIRPORT_INTER_INFO where THREE_WORDS_CODE IN
            ('AMM','HRG','YNB','ABS','BHH','BEY','NJF','TCP','ISU','HOD','SKV','ASW','AQI','ABT','ADE','AJF','TAI','DXB','SSH','MED','TIF','AHB','EBL','ETH','GIZ','AUH','OSM','BSR','RUH','VDA','RIY','AAN','SEW','DOH','KWI','RAH','HOF','XSB','BAH','SHW','TUI','RAE','DWD','DMM','DWC','ULH','URY','TLV','JED','SAH','AZI','BGW','WAE','RKT','GXF','ELQ','XMB','HBE','HMB','ATZ','SHJ','AQJ','KHS','ADJ','FJR','HAS','DAM','RMF','EJH','TUU','EAM','SLL'
            ) AND (td.DEPCODE=THREE_WORDS_CODE or td.ARRCODE=THREE_WORDS_CODE)
        )
        ) zd,

        sum(
        EXISTS
            (
           select THREE_WORDS_CODE from apibase.AIRPORT_INTER_INFO where REGION_CODE='AF'
             AND (td.DEPCODE=THREE_WORDS_CODE or td.ARRCODE=THREE_WORDS_CODE)
        )
        ) fz,

        sum(
        EXISTS (
            select THREE_WORDS_CODE from apibase.AIRPORT_INTER_INFO where REGION_CODE='SA'
            AND (td.DEPCODE=THREE_WORDS_CODE or td.ARRCODE=THREE_WORDS_CODE)
         )
        )NMZ

        FROM TICKET_ORDERDETAIL as td
        join TICKET_ORDER as t
        ON t.ORDERID = td.ORDERID
        where
        t.CREATETIME>=%s
        and t.CREATETIME<=%s
        and t.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        AND IFNULL(td.`LINKTYPE`, 0) != 2
        and INTFLAG=1
        GROUP BY s_day;
    """

    total_query_data = DBCli().sourcedb_cli.queryAll(query_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_total_sql, total_query_data)

    return __file__

if __name__ == "__main__":
    # update_hb_gt_order_daily(1)
    # update_operation_hbgj_order_detail_daily(1)
    # update_hb_gt_order_daily_his()
    # i = 1
    # while 1:
    #     update_hbgj_ticket_region_inter_daily(i)
    #     i += 1

    # update_hbgj_ticket_region_inter_daily(60)
    # update_hbgj_ticket_region_inter_daily(157)
    update_hbgj_ticket_region_inter_daily(423)