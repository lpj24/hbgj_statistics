# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_operation_hbgj_amount_monitor_cz(days=0):
    """更新南航国内外销售额, operation_hbgj_amount_monitor_cz"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days*3))
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1-days))
    cz_inter_inland_sql = """
        SELECT DATE_FORMAT(OD.CREATETIME, '%%Y-%%m-%%d') s_day,
        sum(case when O.PNRSOURCE ='csair' then OD.OUTPAYPRICE else 0 end) CZ_inland_amount,
        sum(case when O.PNRSOURCE ='czint' then OD.OUTPAYPRICE else 0 end) CZ_inter_amount
        FROM TICKET_ORDERDETAIL OD
        LEFT JOIN TICKET_ORDER O ON OD.ORDERID = O.ORDERID
        WHERE
        O.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        AND IFNULL(OD.`LINKTYPE`, 0) != 2
        AND OD.CREATETIME >= %s
        AND OD.CREATETIME < %s
        AND OD.ETICKET IS NOT NULL
        group by s_day
    """

    insert_cz_sql = """
        insert into operation_hbgj_amount_monitor_cz (s_day, CZ_inland_amount, CZ_inter_amount,
        createtime, updatetime)
        values (%s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        CZ_inland_amount = values(CZ_inland_amount),
        CZ_inter_amount = values(CZ_inter_amount)
    """
    dto = [start_date, end_date]
    cz_data = DBCli().sourcedb_cli.queryAll(cz_inter_inland_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_cz_sql, cz_data)
    return __file__


def update_operation_hbgj_amount_monitor_hlth(days=0):
    """更新伙力天汇各航司出票情况, operation_hbgj_amount_monitor_hlth"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 3), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1-days), '%Y-%m-%d')
    hlth_sql = """
        SELECT d.FLYDATE,
        SUBSTR(d.FLYNO,1,2) AS airline,
        sum(case when d.STATE in  (2,54,55) then (d.PRICE + d.AIRPORTFEE + d.RATEFEE) else 0 end) amount,
        sum(case when d.STATE=54 then d.PRICE + d.AIRPORTFEE + d.RATEFEE else 0 end) used_amount
        FROM TICKET_ORDER o
        LEFT JOIN TICKET_ORDERDETAIL d ON o.ORDERID = d.ORDERID
        WHERE o.PNRSOURCE = 'hlth'
        AND (o. MODE IS NULL OR o. MODE = 0)
        AND (
            d.LINKTYPE IS NULL
            OR d.LINKTYPE = '0'
            OR d.LINKTYPE = '1'
        )
        AND d.FLYDATE >= %s
        AND d.FLYDATE < %s
        GROUP BY FLYDATE,airline
    """
    insert_hlth_sql = """
        insert into operation_hbgj_amount_monitor_hlth
        (s_day, airline, amount, used_amount, createtime, updatetime)
        values (%s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        airline = values(airline),
        amount = values(amount),
        used_amount = values(used_amount)
    """
    dto = [start_date, end_date]
    hlth_data = DBCli().sourcedb_cli.queryAll(hlth_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_hlth_sql, hlth_data)
    return __file__


def update_operation_hbgj_amount_monitor_hlth_szx(days=0):
    """更新深圳始发3个航空公司, operation_hbgj_amount_monitor_hlth_SZX"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days*3), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1-days), '%Y-%m-%d')
    szx_sql = """
        SELECT d.FLYDATE,
        SUBSTR(d.FLYNO,1,2) AS airline,
        sum(case when d.STATE in  (2,54,55) then (d.PRICE + d.AIRPORTFEE + d.RATEFEE) else 0 end) amount,
        sum(case when d.STATE=54 then d.PRICE + d.AIRPORTFEE + d.RATEFEE else 0 end) used_amount
        FROM TICKET_ORDER o
        LEFT JOIN TICKET_ORDERDETAIL d ON o.ORDERID = d.ORDERID
        WHERE o.PNRSOURCE = 'hlth'
        AND (o. MODE IS NULL OR o. MODE = 0)
        AND (
            d.LINKTYPE IS NULL
            OR d.LINKTYPE = '0'
            OR d.LINKTYPE = '1'
        )
        AND d.FLYDATE >= %s
        AND d.FLYDATE < %s
        AND SUBSTR(d.FLYNO,1,2) in ('CZ','ZH','HU')
        AND d.DEPCODE = 'SZX'
        GROUP BY FLYDATE,airline
    """

    insert_szx_sql = """
        insert into operation_hbgj_amount_monitor_hlth_SZX
        (s_day, airline, amount, used_amount, createtime, updatetime)
        values (%s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        airline = values(airline),
        amount = values(amount),
        used_amount = values(used_amount)
    """
    dto = [start_date, end_date]
    szx_data = DBCli().sourcedb_cli.queryAll(szx_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_szx_sql, szx_data)


def update_operation_hbgj_amount_monitor_inter(days=0):
    """国际各直销航司出票金额统计/月底清零, operation_hbgj_amount_monitor_inter"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days*3), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1-days), '%Y-%m-%d')
    pn_sql = """
        SELECT left(od.CREATETIME,10),o.PNRSOURCE,PNRSOURCE_CONFIG.`NAME` ,sum(od.OUTPAYPRICE)
        FROM `TICKET_ORDERDETAIL` od
        left JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        left join PNRSOURCE_CONFIG ON o.PNRSOURCE=PNRSOURCE_CONFIG.PNRSOURCE
        where od.CREATETIME>=%s and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        AND IFNULL(od.`LINKTYPE`, 0) != 2
        and o.PNRSOURCE
        in (SELECT PNRSOURCE
        FROM `PNRSOURCE_CONFIG`
        where `SALETYPE`=20)
        AND od.ETICKET IS NOT NULL
        GROUP BY left(od.CREATETIME,10), o.PNRSOURCE
    """
    insert_sql = """
        insert into operation_hbgj_amount_monitor_inter (s_day, pnrsource, pnrsource_name,
        amount, createtime, updatetime)
        values (%s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        pnrsource = values(pnrsource),
        pnrsource_name = values(pnrsource_name),
        amount = values(amount)
    """
    dto = [start_date, end_date]
    inter_company_data = DBCli().sourcedb_cli.queryAll(pn_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_sql, inter_company_data)


def update_operation_hbgj_qp_success(days=0):
    """更新抢票情况统计, operation_hbgj_qp_success"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days*3))
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1-days))
    dto = [start_date, end_date]
    qp_sql = """
        SELECT
        DATE_FORMAT(S.createtime, '%%Y-%%m-%%d'),
        sum(case when S.`status` IN (1,2,3,4) then S.seatnum end) qp,
        sum(case when S.`status`=2 then S.seatnum end) qp_success
        FROM snapped_ticket_task S
        WHERE S.createtime>=%s
        AND S.createtime <%s
        GROUP BY DATE_FORMAT(S.createtime, '%%Y-%%m-%%d');
    """
    insert_sql = """
        insert into operation_hbgj_qp_success (s_day, qp_amount, qp_success_amount,
        createtime, updatetime) values (%s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        qp_amount = values(qp_amount),
        qp_success_amount = values(qp_success_amount)
    """
    qp_data = DBCli().sourcedb_cli.queryAll(qp_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_sql, qp_data)


if __name__ == "__main__":
    i = 1
    while i <= 113:
        update_operation_hbgj_amount_monitor_cz(i)
        update_operation_hbgj_amount_monitor_hlth(i)
        update_operation_hbgj_amount_monitor_hlth_szx(i)
        update_operation_hbgj_amount_monitor_inter(i)
        update_operation_hbgj_qp_success(i)
        i += 1
