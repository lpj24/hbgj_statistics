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


def update_hb_company_income_cost_daily(days=0):
    """更新各个航空公司收入 成本 销售额 机票数量, hb_company_income_cost_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days*1))
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1-days))
    dto = [start_date, end_date]
    income_sql = """
        select DATE_FORMAT(od.CREATETIME,'%%Y-%%m-%%d') s_day, 
        SUBSTR(od.FLYNO,1,2) air_com,
        SUM(case when TYPE=0 AND INCOMETYPE= 0 then INCOME else 0 end) income1,
        SUM(case when TYPE=0 AND INCOMETYPE= 1 then INCOME else 0 end) income2,
        SUM(case when TYPE=0 AND INCOMETYPE= 2 then INCOME else 0 end) income3,
        
        count(case when TYPE=0 AND INCOMETYPE= 0 then o.ORDERID end) income1_ticket,
        count(case when TYPE=0 AND INCOMETYPE= 1 then o.ORDERID end) income2_ticket,
        count(case when TYPE=0 AND INCOMETYPE= 2 then o.ORDERID end) income3_ticket,
        
        sum(case when TYPE=0 AND INCOMETYPE= 0 then od.OUTPAYPRICE else 0 end) income1_amount,
        sum(case when TYPE=0 AND INCOMETYPE= 1 then od.OUTPAYPRICE else 0 end) income2_amount,
        sum(case when TYPE=0 AND INCOMETYPE= 2 then od.OUTPAYPRICE else 0 end) income3_amount
        from TICKET_ORDER_INCOME T_INCOME
        LEFT JOIN TICKET_ORDER_INCOME_TYPE T_TYPE
        ON T_INCOME.PNRSOURCE = T_TYPE.PNRSOURCE
        left join TICKET_ORDER o on T_INCOME.ORDERID=o.ORDERID
        left join `TICKET_ORDERDETAIL` od on T_INCOME.TICKETNO=od.ODID
        where od.CREATETIME>=%s
        and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) 
        and o.INTFLAG=0 AND IFNULL(od.`LINKTYPE`, 0) != 2 
        and od.REFUNDID=0
        and o.`MODE`=0 
        GROUP BY s_day, air_com
    """

    income_data = DBCli().sourcedb_cli.query_all(income_sql, dto)

    cost_sql = """
        select DISTINCT 
        SUM(case when INCOMETYPE= 0 then AMOUNT else 0 end) cost1,
        SUM(case when INCOMETYPE= 1 then AMOUNT else 0 end) cost2,
        SUM(case when INCOMETYPE= 2 then AMOUNT else 0 end) cost3,
        DATE_FORMAT(od.CREATETIME,'%%Y-%%m-%%d') s_day, SUBSTR(od.FLYNO,1,2) air_com
        FROM TICKET_ORDER_COST T_COST
        left join TICKET_ORDER_INCOME_TYPE T_TYPE
        ON T_COST.PNRSOURCE = T_TYPE.PNRSOURCE
        left join skyhotel.`TICKET_ORDER` o
        on T_COST.ORDERID=o.ORDERID
        RIGHT join TICKET_ORDERDETAIL od on T_COST.ODID=od.ODID
        where o.CREATETIME>=%s
        and o.CREATETIME<%s 
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) 
        and o.INTFLAG=0 AND IFNULL(od.`LINKTYPE`, 0) != 2 
        and od.REFUNDID=0
        and o.`MODE`=0 
        group by s_day, air_com
    """
    cost_data = DBCli().sourcedb_cli.query_all(cost_sql, dto)

    insert_sql = """
        insert into hb_company_income_cost_daily (s_day, air_company, incometype0, incometype1, 
        incometype2, tickettype0, tickettype1, tickettype2, amounttype0, amounttype1, amounttype2, 
        costtype0, costtype1, costtype2, createtime, updatetime) values
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0, 0, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        air_company = values(air_company),
        incometype0 = values(incometype0),
        incometype1 = values(incometype1),
        incometype2 = values(incometype2),
        tickettype0 = values(tickettype0),
        tickettype1 = values(tickettype1),
        tickettype2 = values(tickettype2),
        amounttype0 = values(amounttype0),
        amounttype1 = values(amounttype1),
        amounttype2 = values(amounttype2)
    """
    DBCli().targetdb_cli.batch_insert(insert_sql, income_data)

    update_sql = """
        update hb_company_income_cost_daily set costtype0=%s, costtype1=%s, costtype2=%s
        where s_day=%s and air_company=%s
    """
    DBCli().targetdb_cli.batch_insert(update_sql, cost_data)


def update_hb_company_income_cost_supplier_daily(days=0):
    """更新各个航空公司(供应商)收入 成本 销售额 机票数量, hb_company_income_cost_supplier_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 1))
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - days))
    dto = [start_date, end_date]
    income_sql = """
        select DATE_FORMAT(od.CREATETIME,'%%Y-%%m-%%d') s_day, 
        SUBSTR(od.FLYNO,1,2) air_com, o.agentid,
        SUM(case when TYPE=0 AND INCOMETYPE= 0 then INCOME else 0 end) income1,
        SUM(case when TYPE=0 AND INCOMETYPE= 1 then INCOME else 0 end) income2,
        SUM(case when TYPE=0 AND INCOMETYPE= 2 then INCOME else 0 end) income3,

        count(case when TYPE=0 AND INCOMETYPE= 0 then o.ORDERID end) income1_ticket,
        count(case when TYPE=0 AND INCOMETYPE= 1 then o.ORDERID end) income2_ticket,
        count(case when TYPE=0 AND INCOMETYPE= 2 then o.ORDERID end) income3_ticket,

        sum(case when TYPE=0 AND INCOMETYPE= 0 then od.OUTPAYPRICE else 0 end) income1_amount,
        sum(case when TYPE=0 AND INCOMETYPE= 1 then od.OUTPAYPRICE else 0 end) income2_amount,
        sum(case when TYPE=0 AND INCOMETYPE= 2 then od.OUTPAYPRICE else 0 end) income3_amount
        from TICKET_ORDER_INCOME T_INCOME
        LEFT JOIN TICKET_ORDER_INCOME_TYPE T_TYPE
        ON T_INCOME.PNRSOURCE = T_TYPE.PNRSOURCE
        left join TICKET_ORDER o on T_INCOME.ORDERID=o.ORDERID
        left join `TICKET_ORDERDETAIL` od on T_INCOME.TICKETNO=od.ODID
        where od.CREATETIME>=%s
        and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) 
        and o.INTFLAG=0 AND IFNULL(od.`LINKTYPE`, 0) != 2 
        and od.REFUNDID=0
        and o.`MODE`=0 
        GROUP BY s_day, air_com, o.agentid
    """

    income_data = DBCli().sourcedb_cli.query_all(income_sql, dto)

    cost_sql = """
        select DISTINCT 
        SUM(case when INCOMETYPE= 0 then AMOUNT else 0 end) cost1,
        SUM(case when INCOMETYPE= 1 then AMOUNT else 0 end) cost2,
        SUM(case when INCOMETYPE= 2 then AMOUNT else 0 end) cost3,
        DATE_FORMAT(od.CREATETIME,'%%Y-%%m-%%d') s_day, SUBSTR(od.FLYNO,1,2) air_com, o.agentid
        FROM TICKET_ORDER_COST T_COST
        left join TICKET_ORDER_INCOME_TYPE T_TYPE
        ON T_COST.PNRSOURCE = T_TYPE.PNRSOURCE
        left join skyhotel.`TICKET_ORDER` o
        on T_COST.ORDERID=o.ORDERID
        RIGHT join TICKET_ORDERDETAIL od on T_COST.ODID=od.ODID
        where o.CREATETIME>=%s
        and o.CREATETIME<%s 
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) 
        and o.INTFLAG=0 AND IFNULL(od.`LINKTYPE`, 0) != 2 
        and od.REFUNDID=0
        and o.`MODE`=0 
        group by s_day, air_com, o.agentid
    """
    cost_data = DBCli().sourcedb_cli.query_all(cost_sql, dto)

    insert_sql = """
        insert into hb_company_income_cost_supplier_daily (s_day, air_company, agentid, incometype0, incometype1, 
        incometype2, tickettype0, tickettype1, tickettype2, amounttype0, amounttype1, amounttype2, 
        costtype0, costtype1, costtype2, createtime, updatetime) values
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0, 0, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        air_company = values(air_company),
        agentid = values(agentid),
        incometype0 = values(incometype0),
        incometype1 = values(incometype1),
        incometype2 = values(incometype2),
        tickettype0 = values(tickettype0),
        tickettype1 = values(tickettype1),
        tickettype2 = values(tickettype2),
        amounttype0 = values(amounttype0),
        amounttype1 = values(amounttype1),
        amounttype2 = values(amounttype2)
    """
    DBCli().targetdb_cli.batch_insert(insert_sql, income_data)

    update_sql = """
        update hb_company_income_cost_supplier_daily set costtype0=%s, costtype1=%s, costtype2=%s
        where s_day=%s and air_company=%s and agentid=%s
    """
    DBCli().targetdb_cli.batch_insert(update_sql, cost_data)


def update_hb_company_income_cost_nation_daily(days=0):
    """更新各个航空公司 利润 销售额 机票数量, hb_company_income_cost_nation_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 1), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - days), '%Y-%m-%d')
    dto = [start_date, end_date]
    income_sql = """
        SELECT DATE_FORMAT(od.CREATETIME,'%%Y-%%m-%%d') s_day, 
        SUBSTR(`flyno`,1,2) air_com,  
        SUM(case when INCOMETYPE =3 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_0,
        SUM(case when INCOMETYPE =3 then 1 else 0 end) ticket_count_0,
        SUM(case when INCOMETYPE =3 then od.PRICE else 0 end) amount_0,
        SUM(case when INCOMETYPE =1 then 1 else 0 end) ticket_count_1,
        SUM(case when INCOMETYPE =1 then od.PRICE else 0 end) amount_1,
        SUM(case when INCOMETYPE =2 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_2,
        SUM(case when INCOMETYPE =2 then 1 else 0 end) ticket_count_2,
        SUM(case when INCOMETYPE =2 then od.PRICE else 0 end) amount_2
        from TICKET_ORDER o
        left join `TICKET_ORDERDETAIL` od on o.orderid=od.orderid
        left join TICKET_ORDER_INCOME_TYPE T_TYPE 
        ON o.PNRSOURCE = T_TYPE.PNRSOURCE 
        where od.CREATETIME>=%s
        and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) 
        and o.INTFLAG=0 AND IFNULL(od.`LINKTYPE`, 0) != 2 
        and od.REFUNDID=0
        and o.`MODE`=0  
        and OLD_ORDERID is null 
        group by s_day,air_com
        order by s_day 
    """

    income_data = DBCli().sourcedb_cli.query_all(income_sql, dto)

    insert_sql = """
        insert into hb_company_income_cost_nation_daily (s_day, air_company, profittype0, tickettype0, amounttype0,
        tickettype1, amounttype1,profittype2, tickettype2, amounttype2, createtime, updatetime)
       values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        air_company = values(air_company),
        profittype0 = values(profittype0),
        tickettype0 = values(tickettype0),
        amounttype0 = values(amounttype0),

        tickettype1 = values(tickettype1),
        amounttype1 = values(amounttype1),

        profittype2 = values(profittype2),
        tickettype2 = values(tickettype2),
        amounttype2 = values(amounttype2)
    """
    DBCli().targetdb_cli.batch_insert(insert_sql, income_data)

    income1_sql = """
           SELECT SUBSTR(`flyno`,1,2),
           sum(income) FROM TICKET_ORDER_INCOME i JOIN  TICKET_ORDER_INCOME_TYPE t on i.PNRSOURCE=t.PNRSOURCE 
           join TICKET_ORDERDETAIL d on i.TICKETNO=d.ODID
           JOIn TICKET_ORDER o on i.ORDERID=o.ORDERID
           WHERE
           t.INCOMETYPE=1 and i.type=0 and o.mode=0 and i.INTFLAG=0
           and INCOMEDATE>=%s and INCOMEDATE<%s and d.REFUNDID=0 
           and OLD_ORDERID is null AND IFNULL(d.`LINKTYPE`, 0) != 2  
           GROUP BY SUBSTR(`flyno`,1,2)
           ORDER BY SUBSTR(`flyno`,1,2);

       """

    cost1_sql = """
           SELECT SUBSTR(`flyno`,1,2),
           sum(AMOUNT) 
           FROM TICKET_ORDER_COST c JOIN  TICKET_ORDER_INCOME_TYPE t on c.PNRSOURCE=t.PNRSOURCE 
           join TICKET_ORDERDETAIL d on c.odid=d.ODID
           JOIn TICKET_ORDER o on c.ORDERID=o.ORDERID
           WHERE
           t.INCOMETYPE=1 and c.type=0 and 
           c.amounttype not in (2,3) and c.costtype=0 and o.mode=0 and c.INTFLAG=0
           and COSTDATE>=%s and COSTDATE<%s and d.REFUNDID=0  
           and OLD_ORDERID is null 
           AND IFNULL(d.`LINKTYPE`, 0) != 2 GROUP BY SUBSTR(`flyno`,1,2)
           ORDER BY SUBSTR(`flyno`,1,2);
       """

    update_profit1_sql = """
           update hb_company_income_cost_nation_daily set profittype1=%s
           where air_company=%s and s_day=%s
       """
    income1_data = dict(DBCli().sourcedb_cli.query_all(income1_sql, dto))
    cost1_data = dict(DBCli().sourcedb_cli.query_all(cost1_sql, dto))
    update_profit1 = []
    for k, v in income1_data.items():
        update_profit1.append([float(v) - float(cost1_data.get(k, 0)), k, start_date])

    DBCli().targetdb_cli.batch_insert(update_profit1_sql, update_profit1)


def update_hb_company_income_cost_inter_daily(days=0):
    """更新各个航空公司 利润 销售额 机票数量, hb_company_income_cost_inter_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 1), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - days), '%Y-%m-%d')
    dto = [start_date, end_date]
    income_sql = """
        SELECT DATE_FORMAT(od.CREATETIME,'%%Y-%%m-%%d') s_day, 
        SUBSTR(`flyno`,1,2) air_com,  
        SUM(case when INCOMETYPE =3 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_0,
        SUM(case when INCOMETYPE =3 then 1 else 0 end) ticket_count_0,
        SUM(case when INCOMETYPE =3 then od.PRICE else 0 end) amount_0,
        SUM(case when INCOMETYPE =1 then 1 else 0 end) ticket_count_1,
        SUM(case when INCOMETYPE =1 then od.PRICE else 0 end) amount_1,
        SUM(case when INCOMETYPE =2 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_2,
        SUM(case when INCOMETYPE =2 then 1 else 0 end) ticket_count_2,
        SUM(case when INCOMETYPE =2 then od.PRICE else 0 end) amount_2
        from TICKET_ORDER o
        left join `TICKET_ORDERDETAIL` od on o.orderid=od.orderid
        left join TICKET_ORDER_INCOME_TYPE T_TYPE 
        ON o.PNRSOURCE = T_TYPE.PNRSOURCE 
        where od.CREATETIME>=%s
        and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) 
        and o.INTFLAG=1 AND IFNULL(od.`LINKTYPE`, 0) != 2 
        and od.REFUNDID=0
        and o.`MODE`=0  
        and OLD_ORDERID is null 
        group by s_day,air_com
        order by s_day 
    """

    income_data = DBCli().sourcedb_cli.query_all(income_sql, dto)

    insert_sql = """
        insert into hb_company_income_cost_inter_daily (s_day, air_company, profittype0, tickettype0, amounttype0,
        tickettype1, amounttype1,profittype2, tickettype2, amounttype2, createtime, updatetime) 
       values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        air_company = values(air_company),
        profittype0 = values(profittype0),
        tickettype0 = values(tickettype0),
        amounttype0 = values(amounttype0),

        tickettype1 = values(tickettype1),
        amounttype1 = values(amounttype1),

        profittype2 = values(profittype2),
        tickettype2 = values(tickettype2),
        amounttype2 = values(amounttype2)
    """
    DBCli().targetdb_cli.batch_insert(insert_sql, income_data)

    income1_sql = """
        SELECT SUBSTR(`flyno`,1,2),
        sum(income) FROM TICKET_ORDER_INCOME i JOIN  TICKET_ORDER_INCOME_TYPE t on i.PNRSOURCE=t.PNRSOURCE 
        join TICKET_ORDERDETAIL d on i.TICKETNO=d.ODID
        JOIn TICKET_ORDER o on i.ORDERID=o.ORDERID
        WHERE
        t.INCOMETYPE=1 and i.type=0 and o.mode=0 and i.INTFLAG=1
        and INCOMEDATE>=%s and INCOMEDATE<%s and d.REFUNDID=0 
        and OLD_ORDERID is null AND IFNULL(d.`LINKTYPE`, 0) != 2  
        GROUP BY SUBSTR(`flyno`,1,2)
        ORDER BY SUBSTR(`flyno`,1,2);

    """

    cost1_sql = """
        SELECT SUBSTR(`flyno`,1,2),
        sum(AMOUNT) 
        FROM TICKET_ORDER_COST c JOIN  TICKET_ORDER_INCOME_TYPE t on c.PNRSOURCE=t.PNRSOURCE 
        join TICKET_ORDERDETAIL d on c.odid=d.ODID
        JOIn TICKET_ORDER o on c.ORDERID=o.ORDERID
        WHERE
        t.INCOMETYPE=1 and c.type=0 and 
        c.amounttype not in (2,3) and c.costtype=0 and o.mode=0 and c.INTFLAG=1
        and COSTDATE>=%s and COSTDATE<%s and d.REFUNDID=0  
        and OLD_ORDERID is null 
        AND IFNULL(d.`LINKTYPE`, 0) != 2 GROUP BY SUBSTR(`flyno`,1,2)
        ORDER BY SUBSTR(`flyno`,1,2);
    """

    update_profit1_sql = """
        update hb_company_income_cost_inter_daily set profittype1=%s
        where air_company=%s and s_day=%s
    """
    income1_data = dict(DBCli().sourcedb_cli.query_all(income1_sql, dto))
    cost1_data = dict(DBCli().sourcedb_cli.query_all(cost1_sql, dto))
    update_profit1 = []
    for k, v in income1_data.items():
        update_profit1.append([float(v) - float(cost1_data.get(k, 0)), k, start_date])

    DBCli().targetdb_cli.batch_insert(update_profit1_sql, update_profit1)


if __name__ == "__main__":
    # update_operation_hbgj_amount_monitor_cz(1)
    i = 1
    while i <= 474:
        update_hb_company_income_cost_inter_daily(i)
        update_hb_company_income_cost_nation_daily(i)
        i += 1