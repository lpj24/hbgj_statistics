# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_operation_hbgj_amount_monitor_cz(days=0):
    """南航国内外销售额, operation_hbgj_amount_monitor_cz"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days*30))
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1-days))
    cz_inter_inland_sql = """
        SELECT DATE_FORMAT(OD.CREATETIME, '%%Y-%%m-%%d') s_day,
        sum(case when O.PNRSOURCE in ('csair','csairnew') and O.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        then OD.OUTPAYPRICE else 0 end) CZ_inland_amount,
        sum(case when O.PNRSOURCE ='czint' and O.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
         then OD.OUTPAYPRICE else 0 end) CZ_inter_amount,
        sum(case when O.PNRSOURCE in ('csair','csairnew') and O.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31,52,71)
        and IFNULL(OD.REFUNDID, 0) != 0
        then OD.OUTPAYPRICE else 0 end) CZ_inland_amount_return,
        sum(case when O.PNRSOURCE ='czint' and O.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31,52,71)
        and IFNULL(OD.REFUNDID, 0) != 0
        then OD.OUTPAYPRICE else 0 end) CZ_inter_amount_return
        FROM TICKET_ORDERDETAIL OD
        LEFT JOIN TICKET_ORDER O ON OD.ORDERID = O.ORDERID
        WHERE IFNULL(OD.`LINKTYPE`, 0) != 2
        and OD.CREATETIME >= %s
        AND OD.CREATETIME < %s
        AND OD.ETICKET IS NOT NULL
        group by s_day;
    """

    insert_cz_sql = """
        insert into operation_hbgj_amount_monitor_cz (s_day, CZ_inland_amount, CZ_inter_amount,
        CZ_inland_amount_return, CZ_inter_amount_return,
        createtime, updatetime)
        values (%s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        CZ_inland_amount = values(CZ_inland_amount),
        CZ_inter_amount = values(CZ_inter_amount),
        CZ_inland_amount_return = values(CZ_inland_amount_return),
        CZ_inter_amount_return = values(CZ_inter_amount_return)
    """
    dto = [start_date, end_date]
    cz_data = DBCli().sourcedb_cli.query_all(cz_inter_inland_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_cz_sql, cz_data)


def update_operation_hbgj_amount_monitor_hlth(days=0):
    """伙力天汇各航司出票情况, operation_hbgj_amount_monitor_hlth"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 20), '%Y-%m-%d')
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
    hlth_data = DBCli().sourcedb_cli.query_all(hlth_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_hlth_sql, hlth_data)


def update_operation_hbgj_amount_monitor_hlth_szx(days=0):
    """深圳始发3个航空公司, operation_hbgj_amount_monitor_hlth_SZX"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days*20), '%Y-%m-%d')
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
    szx_data = DBCli().sourcedb_cli.query_all(szx_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_szx_sql, szx_data)


def update_operation_hbgj_amount_monitor_inter(days=0):
    """国际各直销航司出票金额统计/月底清零, operation_hbgj_amount_monitor_inter"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days*20), '%Y-%m-%d')
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
    inter_company_data = DBCli().sourcedb_cli.query_all(pn_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, inter_company_data)


def update_operation_hbgj_qp_success(days=0):
    """抢票情况统计, operation_hbgj_qp_success"""
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
        insert into operation_hbgj_qp_success (s_day, qp_count, qp_success_count,
        createtime, updatetime) values (%s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        qp_count = values(qp_count),
        qp_success_count = values(qp_success_count)
    """
    qp_data = DBCli().sourcedb_cli.query_all(qp_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, qp_data)


def update_operation_hbgj_special_return_daily(days=1):
    """自营特殊产品返现额, operation_hbgj_special_return"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days*3))
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1-days))
    dto = [start_date, end_date]
    insert_sql = """
        insert into operation_hbgj_special_return
        (s_day, type, name, amount, createtime, updatetime)
        values
        (%s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        type = values(type),
        name = values(name),
        amount = values(amount)
    """
    agency_fee_sql = """
        SELECT left(od.CREATETIME,10), 'agency_fee' as type, '代理费' as name,
        count(DISTINCT(eticket))*80 as '代理费' FROM `TICKET_ORDERDETAIL` od
        INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        where od.CREATETIME>=%s
        and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        AND IFNULL(od.`LINKTYPE`, 0) != 2
        and SUBSTR(`flyno`,1,2)='HU'
        and od.eticket is not null
        and pnrsource='hlth'
        and cabin='W'
        and o.ORDERSTATUE not like '%6%' GROUP BY left(od.CREATETIME,10),pnrsource;
    """
    agency_fee = DBCli().sourcedb_cli.query_all(agency_fee_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, agency_fee)

    zh_ryx_sql = """
        SELECT left(createtime,10), 'ZH_ryx' as type, '深航商务如意行' name,sum(rtcash) as 返现总额
        FROM `ticket_cashback` where SUBSTR(`flyno`,1,2)='ZH'
        and CREATETIME>=%s
        and CREATETIME<%s
        and `status`<>0
        and `status`<>4
        GROUP BY left(createtime,10);
    """

    zh_ryx = DBCli().sourcedb_cli.query_all(zh_ryx_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, zh_ryx)

    ho_sw_sql = """
        SELECT left(createtime,10),'ho_sw' as type, '吉祥商务优选' as name, sum(rtcash) as 返现总额
        FROM `ticket_cashback` where SUBSTR(`flyno`,1,2)='HO'
        and CREATETIME>=%s
        and CREATETIME<%s
        and `status`<>0 and `status`<>4
        GROUP BY left(createtime,10);
    """
    ho_sw = DBCli().sourcedb_cli.query_all(ho_sw_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, ho_sw)

    u_w_sql = """
            SELECT left(createtime,10),'3U_w' as type, '川航W舱' as name, sum(rtcash) as 返现总额
            FROM `ticket_cashback` where SUBSTR(`flyno`,1,2)='3U'
            and CREATETIME>=%s
            and CREATETIME<%s
            and `status`<>0 and `status`<>4
            AND cabin='W'
            GROUP BY left(createtime,10)
    """
    u_w = DBCli().sourcedb_cli.query_all(u_w_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, u_w)

    u_not_w_sql = """
        SELECT left(createtime,10),'3U_!w' as type, '川航免改易行&里程多送' as name,
        sum(rtcash) as 返现总额
        FROM `ticket_cashback`
        where SUBSTR(`flyno`,1,2)='3U'
        and CREATETIME>=%s
        and CREATETIME<%s
        and `status`<>0 and `status`<>4
        AND cabin!='W'
        GROUP BY left(createtime,10)
    """
    u_not_w = DBCli().sourcedb_cli.query_all(u_not_w_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, u_not_w)

    gx_sql = """
        SELECT left(createtime,10),'GX' AS type, '北部湾' as name,sum(rtcash)*0.4 as 返现总额
        FROM `ticket_cashback`
        where SUBSTR(`flyno`,1,2)='GX'
        and CREATETIME>=%s
        and CREATETIME<%s
        and `status`<>0
        and `status`<>4
        GROUP BY left(createtime,10);
    """
    gx = DBCli().sourcedb_cli.query_all(gx_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, gx)

if __name__ == "__main__":
    # update_operation_hbgj_amount_monitor_cz(1)
    update_operation_hbgj_amount_monitor_cz(1)