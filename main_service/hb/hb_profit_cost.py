# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import requests
import sys
reload(sys)
sys.setdefaultencoding('utf8')


def update_hb_car_hotel_profit(days=0):
    """更新航班专车酒店成本(德付通9.1日以前系数是0.005以后是0.0018),
    profit_hb_cost profit_huoli_car_cost profit_huoli_hotel_cost"""
    query_date = DateUtil.get_date_before_days(days * 7)
    today = DateUtil.get_date_after_days(1 - days)
    sql = """
        select distinct TRADE_TIME s_day,
        sum(case when (AMOUNT_TYPE=2 and PRODUCT='0' and TRADE_CHANNEL not like '%%coupon%%') then amount else 0 end) paycost_in,
        sum(case when (AMOUNT_TYPE=3 and PRODUCT='0' and TRADE_CHANNEL not like '%%coupon%%') then amount else 0 end) paycost_return,
        sum(case when (AMOUNT_TYPE=1 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST)='0' and
        (PRODUCT=COST OR COST IS NULL) and TRADE_CHANNEL='coupon')
        then amount else 0 end) coupon_in,
        sum(case when (AMOUNT_TYPE=4 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST)='0' and
        (PRODUCT=COST OR COST IS NULL) and TRADE_CHANNEL='coupon')
        then amount else 0 end) coupon_return,
        sum(case when (AMOUNT_TYPE=1 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST)='0' and
        PRODUCT!=COST and TRADE_CHANNEL='coupon')
        then amount else 0 end) else_coupon_in,
        sum(case when (AMOUNT_TYPE=4 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST)='0' and
        PRODUCT!=COST and TRADE_CHANNEL='coupon')
        then amount else 0 end) else_coupon_return,
        sum(case when (AMOUNT_TYPE=6 and PRODUCT='20') then amount else 0 end) delay_care,
        sum(case when (AMOUNT_TYPE=5 and PRODUCT in ('1')) then amount else 0 end) point_give_amount,
        sum(case when (AMOUNT_TYPE=6 and PRODUCT in ('6','8','24','25')) then amount else 0 end) balance_give_amount
        from PAY_COST_INFO where TRADE_TIME>=%s and TRADE_TIME<%s
        group by TRADE_TIME
    """
    dto = [query_date, today]
    result = DBCli().pay_cost_cli.query_all(sql, dto)
    other_cost_sql = """
        select
        sum(case when T_COST.INTFLAG=0 and AMOUNTTYPE in (0, 1) and INCOMETYPE= 0 then AMOUNT else 0 end) inland_price_diff_type0,
        sum(case when T_COST.INTFLAG=0 and AMOUNTTYPE in (0, 1) and INCOMETYPE= 1 then AMOUNT else 0 end) inland_price_diff_type1,
        sum(case when AMOUNTTYPE in (2, 3) then AMOUNT else 0 end) inland_refund_new,
        sum(case when T_COST.INTFLAG=1 and AMOUNTTYPE in (0, 1) then AMOUNT else 0 end) inter_price_diff,
        COSTDATE
        FROM TICKET_ORDER_COST T_COST
        left join TICKET_ORDER_INCOME_TYPE T_TYPE
        ON T_COST.PNRSOURCE = T_TYPE.PNRSOURCE
        where COSTDATE>=%s and COSTDATE<%s
        GROUP BY COSTDATE
        ORDER BY COSTDATE
    """

    inland_price_diff_type2_sql = """
        SELECT sum(c.amount), c.COSTDATE
        FROM `TICKET_ORDER_COST` c
        join skyhotel.`TICKET_ORDERDETAIL` od on c.ODID=od.ODID
        left join skyhotel.`TICKET_ORDER` o
        on c.ORDERID=o.ORDERID
        where  c.PNRSOURCE='hlth' and c.type=0
        and c.COSTDATE>=%s
        and c.COSTDATE<%s
        and c.AMOUNTTYPE!=2
        and od.LINKTYPE is NULL and o.mode=0
        and od.LINKDETAILID=0 GROUP BY  COSTDATE;
    """
    inland_price_diff_type2 = DBCli().sourcedb_cli.query_all(inland_price_diff_type2_sql, dto)
    other_result = DBCli().sourcedb_cli.query_all(other_cost_sql, dto)
    update_other_cost_sql = """
        update profit_hb_cost set inland_price_diff_type0=%s, inland_price_diff_type1=%s,
        inland_refund_new=%s, inter_price_diff=%s where s_day=%s
    """

    update_inland_price_diff_type2_sql = """
        update profit_hb_cost set inland_price_diff_type2=%s where s_day=%s
    """

    query_dft_cost_sql = """
        SELECT
        case when
            DATE_FORMAT(od.CREATETIMe, '%%Y-%%m-%%d') >= '2017-09-01'
                then sum(od.REALPRICE +  od.AIRPORTFEE)*0.0018
            else sum(od.REALPRICE +  od.AIRPORTFEE)*0.005
        end as dft_amount,
        DATE_FORMAT(od.CREATETIMe, '%%Y-%%m-%%d') s_day
        FROM `TICKET_ORDERDETAIL` od
        INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        where od.CREATETIMe>=%s
        and od.CREATETIMe<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        AND IFNULL(od.`LINKTYPE`, 0) != 2
        and o.PNRSOURCE='hlth'
        and o.SUBORDERNO='BOP' and
        od.ETICKET is not null
        group by s_day
    """

    update_dft_cost_sql = """
        update profit_hb_cost set dft_cost=%s where s_day=%s
    """
    dft_result = DBCli().sourcedb_cli.query_all(query_dft_cost_sql, dto)

    cut_point_sql = """
        SELECT
        -sum(case when CHANNELTYPE=5 then ud.POINTS else 0 end)/1000 point_type5_amount,
        -sum(case when CHANNELTYPE=6 then ud.POINTS else 0 end)/1000 point_type6_amount,
        DATE_FORMAT(ud.CREATE_TIME,'%%Y-%%m-%%d') s_day
        FROM USER_POINTS_DETAIL ud
        LEFT JOIN USER_POINTS u on ud.POINT_ID=u.ID
        WHERE u.TYPE=1 AND ud.TYPE='2'
        AND ud.CREATE_TIME>=%s
        AND ud.CREATE_TIME<%s
        GROUP BY s_day
    """

    update_cut_point_cost_sql = """
        update profit_hb_cost set point_type5_amount=%s, point_type6_amount=%s
        where s_day=%s
    """

    cut_point_amount = DBCli().sourcedb_cli.query_all(cut_point_sql, dto)
    insert_sql = """
        insert into profit_hb_cost (s_day, paycost_in, paycost_return, coupon_in, coupon_return,
        else_coupon_in, else_coupon_return, delay_care, point_give_amount, balance_give_amount, createtime, updatetime) values (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()
        )
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        paycost_in = VALUES(paycost_in),
        paycost_return = VALUES(paycost_return),
        coupon_in = VALUES(coupon_in),
        coupon_return = VALUES(coupon_return),
        else_coupon_in = VALUES(else_coupon_in),
        else_coupon_return = VALUES(else_coupon_return),
        delay_care = VALUES(delay_care),
        point_give_amount = VALUES(point_give_amount),
        balance_give_amount = VALUES(balance_give_amount)
    """
    DBCli().targetdb_cli.batch_insert(insert_sql, result)
    DBCli().targetdb_cli.batch_insert(update_other_cost_sql, other_result)
    DBCli().targetdb_cli.batch_insert(update_dft_cost_sql, dft_result)
    DBCli().targetdb_cli.batch_insert(update_inland_price_diff_type2_sql, inland_price_diff_type2)
    DBCli().targetdb_cli.batch_insert(update_cut_point_cost_sql, cut_point_amount)

    car_sql = """
        select distinct TRADE_TIME s_day,
        sum(case when (AMOUNT_TYPE=2 and PRODUCT='7' and TRADE_CHANNEL not like '%%coupon%%') then amount else 0 end) paycost_in,
        sum(case when (AMOUNT_TYPE=3 and PRODUCT='7' and TRADE_CHANNEL not like '%%coupon%%') then amount else 0 end) paycost_return,
        sum(case when (AMOUNT_TYPE=1 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST)='7' and
        (PRODUCT=COST OR COST IS NULL) and TRADE_CHANNEL='coupon') then amount else 0 end) coupon_in,
        sum(case when (AMOUNT_TYPE=4 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST)='7' and
        (PRODUCT=COST OR COST IS NULL) and TRADE_CHANNEL='coupon') then amount else 0 end) coupon_return,
        sum(case when (AMOUNT_TYPE=1 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST)='7' and
        PRODUCT!=COST and TRADE_CHANNEL='coupon') then amount else 0 end) else_coupon_in,
        sum(case when (AMOUNT_TYPE=4 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST)='7' and
        PRODUCT!=COST and TRADE_CHANNEL='coupon') then amount else 0 end) else_coupon_return,
        sum(case when (AMOUNT_TYPE=5 and PRODUCT in ('5','13')) then amount else 0 end) point_give_amount,
        sum(case when (AMOUNT_TYPE=6 and PRODUCT in ('12','29')) then amount else 0 end) balance_give_amount
        from PAY_COST_INFO where TRADE_TIME>=%s and TRADE_TIME<%s
        group by s_day
    """

    result = DBCli().pay_cost_cli.query_all(car_sql, dto)
    insert_car_sql = """
        insert into profit_huoli_car_cost (s_day, paycost_in, paycost_return, coupon_in, coupon_return,
        else_coupon_in, else_coupon_return, point_give_amount, balance_give_amount,
        createtime, updatetime) values (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()
        )
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        paycost_in = VALUES(paycost_in),
        paycost_return = VALUES(paycost_return),
        coupon_in = VALUES(coupon_in),
        coupon_return = VALUES(coupon_return),
        else_coupon_in = VALUES(else_coupon_in),
        else_coupon_return = VALUES(else_coupon_return),
        point_give_amount = VALUES(point_give_amount),
        balance_give_amount = VALUES(balance_give_amount)
    """
    DBCli().targetdb_cli.batch_insert(insert_car_sql, result)

    hotel_sql = """
        select distinct TRADE_TIME s_day,
        sum(case when (AMOUNT_TYPE=2 and PRODUCT='36' and TRADE_CHANNEL not like '%%coupon%%') then amount else 0 end) paycost_in,
        sum(case when (AMOUNT_TYPE=3 and PRODUCT='36' and TRADE_CHANNEL not like '%%coupon%%') then amount else 0 end) paycost_return,
        sum(case when (AMOUNT_TYPE=1 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST)='36' and
        (PRODUCT=COST OR COST IS NULL) and TRADE_CHANNEL='coupon') then amount else 0 end) coupon_in,
        sum(case when (AMOUNT_TYPE=4 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST)='36' and
        (PRODUCT=COST OR COST IS NULL) and TRADE_CHANNEL='coupon') then amount else 0 end) coupon_return,
        sum(case when (AMOUNT_TYPE=1 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST)='36' and
        PRODUCT!=COST and TRADE_CHANNEL='coupon') then amount else 0 end) else_coupon_in,
        sum(case when (AMOUNT_TYPE=4 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST)='36' and
        PRODUCT!=COST and TRADE_CHANNEL='coupon') then amount else 0 end) else_coupon_return,
        sum(case when (AMOUNT_TYPE=5 and PRODUCT in ('8')) then amount else 0 end) point_give_amount,
        sum(case when (AMOUNT_TYPE=6 and PRODUCT in ('9','10')) then amount else 0 end) balance_give_amount
        from PAY_COST_INFO where TRADE_TIME>=%s and TRADE_TIME<%s
        group by s_day
    """

    result = DBCli().pay_cost_cli.query_all(hotel_sql, dto)
    insert_hotel_sql = """
        insert into profit_huoli_hotel_cost (s_day, paycost_in, paycost_return, coupon_in, coupon_return,
        else_coupon_in, else_coupon_return, point_give_amount, balance_give_amount,
        createtime, updatetime) values (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()
        )
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        paycost_in = VALUES(paycost_in),
        paycost_return = VALUES(paycost_return),
        coupon_in = VALUES(coupon_in),
        coupon_return = VALUES(coupon_return),
        else_coupon_in = VALUES(else_coupon_in),
        else_coupon_return = VALUES(else_coupon_return),
        point_give_amount = VALUES(point_give_amount),
        balance_give_amount = VALUES(balance_give_amount)
    """
    DBCli().targetdb_cli.batch_insert(insert_hotel_sql, result)


def update_car_cost_detail(days=0):
    """更新专车成本明细, profit_huoli_car_cost_type"""
    query_date = DateUtil.get_date_before_days(days * 7)
    today = DateUtil.get_date_after_days(1 - days)
    dto = [query_date, today]
    car_sql = """
        select distinct TRADE_TIME s_day,
        sum(case when (AMOUNT_TYPE=2 and PRODUCT='7' and TRADE_CHANNEL not like '%%coupon%%') then amount else 0 end) paycost_in,
        sum(case when (AMOUNT_TYPE=3 and PRODUCT='7' and TRADE_CHANNEL not like '%%coupon%%') then amount else 0 end) paycost_return,
        sum(case when (AMOUNT_TYPE=1 and PRODUCT='7' and TRADE_CHANNEL like '%%coupon%%') then amount else 0 end) coupon_in,
        sum(case when (AMOUNT_TYPE=4 and PRODUCT='7' and TRADE_CHANNEL like '%%coupon%%') then amount else 0 end) coupon_return,
        sum(case when (AMOUNT_TYPE=5 and PRODUCT in ('5','13')) then amount else 0 end) point_give_amount,
        sum(case when (AMOUNT_TYPE=6 and PRODUCT in ('12','29')) then amount else 0 end) balance_give_amount
        from PAY_COST_INFO where TRADE_TIME>=%s and TRADE_TIME<%s
        group by s_day
    """

    insert_sql = """
        insert into profit_huoli_car_cost_type (s_day, cost_type, amount, createtime, updatetime)
        values (%s, %s, %s, now(), now())
        ON DUPLICATE KEY UPDATE updatetime = now(),
        s_day = values(s_day),
        cost_type = values(cost_type),
        amount = values(amount)
    """
    insert_car_cost = []
    result = DBCli(dict).pay_cost_cli.query_all(car_sql, dto)
    for cost_data in result:
        pay_cost_in = float(cost_data["paycost_in"]) - float(cost_data["paycost_return"])
        coupon_in = cost_data["coupon_in"] - cost_data["coupon_return"]
        point_give_amount = cost_data["point_give_amount"]
        balance_give_amount = cost_data["balance_give_amount"]
        s_day = DateUtil.date2str(cost_data["s_day"], '%Y-%m-%d')
        insert_car_cost.append((s_day, u"支付成本", pay_cost_in))
        insert_car_cost.append((s_day, u"优惠券使用金额", coupon_in))
        insert_car_cost.append((s_day, u"返还积分金额", point_give_amount))
        insert_car_cost.append((s_day, u"赠送余额", balance_give_amount))
        url = "http://120.133.0.170:8070/mall/bi/costdetail"
        params = {"beginDate": s_day,
                  "endDate": DateUtil.date2str(DateUtil.add_days(cost_data["s_day"], 1), '%Y-%m-%d')}
        car_result = requests.get(url, params=params).json()
        car_result = car_result["result"]
        for car_cost_data in car_result:
            try:
                car_date = car_cost_data["date"]
                cost_type = car_cost_data["type"]
                cost_amount = car_cost_data["amount"]
                insert_car_cost.append((car_date, cost_type, cost_amount))
            except KeyError:
                continue
    DBCli().targetdb_cli.batch_insert(insert_sql, insert_car_cost)
    pass


def update_huoli_car_income_daily(days=0):
    """更新伙力专车收入, profit_huoli_car_income"""
    query_date = DateUtil.get_date_before_days(days)
    today = DateUtil.get_date_after_days(1 - days)
    insert_car_sql = """
        insert into profit_huoli_car_income (s_day, income, createtime, updatetime) values (
            %s, %s, now(), now()
        )
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        income = VALUES(income)
    """
    import requests
    url = "http://120.133.0.170:8070/mall/bi/income"
    params = {"beginDate": DateUtil.date2str(query_date, '%Y-%m-%d'), "endDate": DateUtil.date2str(today, '%Y-%m-%d')}
    car_result = requests.get(url, params=params).json()
    car_result = car_result["result"][0]
    DBCli().targetdb_cli.insert(insert_car_sql, [car_result['date'], car_result['income']])
    pass


def update_huoli_car_income_type(days=0):
    """更新专车的收入类型, profit_huoli_car_income_type"""
    query_date = DateUtil.get_date_before_days(days)
    today = DateUtil.get_date_after_days(1 - days)
    insert_car_sql = """
        insert into profit_huoli_car_income_type (s_day, type, income, createtime, updatetime) values (
            %s, %s, %s,  now(), now()
        )
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        type = VALUES(type),
        income = VALUES(income)
    """
    import requests
    url = "http://120.133.0.170:8070/mall/bi/incomedetail"
    params = {"beginDate": DateUtil.date2str(query_date, '%Y-%m-%d'), "endDate": DateUtil.date2str(today, '%Y-%m-%d')}
    car_result = requests.get(url, params=params).json()
    car_result = car_result["result"]
    insert_car_income = []
    for car_income_data in car_result:
        car_date = car_income_data["date"]
        income_type = car_income_data["type"]
        income_amount = car_income_data["amount"]
        insert_car_income.append((car_date, income_type, income_amount))
    DBCli().targetdb_cli.batch_insert(insert_car_sql, insert_car_income)


def update_profit_hb_income(days=0):
    """更新航班收入, profit_hb_income"""
    query_date = DateUtil.get_date_before_days(days*5)
    today = DateUtil.get_date_after_days(1 - days)
    sql = """
        SELECT INCOMEDATE,
        SUM(case when TYPE=0 AND T_INCOME.INTFLAG=0 AND INCOMETYPE= 0 THEN INCOME else 0 END) inland_ticket_incometype0,
        SUM(case when TYPE=0 AND T_INCOME.INTFLAG=0 AND INCOMETYPE= 1 THEN INCOME else 0 END) inland_ticket_incometype1,
        SUM(case when TYPE=0 AND T_INCOME.INTFLAG=0 AND INCOMETYPE= 2 THEN INCOME else 0 END) inland_ticket_incometype2,
        sum(case when TYPE=0 AND T_INCOME.INTFLAG=1 THEN INCOME else 0 END) inter_ticket_income,
        sum(case when TYPE=1 AND T_INCOME.INTFLAG=0 THEN INCOME else 0 END) inland_insure_income,
        sum(case when TYPE=1 AND T_INCOME.INTFLAG=1 THEN INCOME else 0 END) inter_insure_income
        FROM TICKET_ORDER_INCOME T_INCOME
        left join TICKET_ORDER_INCOME_TYPE T_TYPE
        ON T_INCOME.PNRSOURCE = T_TYPE.PNRSOURCE
        WHERE INCOMEDATE>=%s and INCOMEDATE<%s
        GROUP BY INCOMEDATE
        ORDER BY INCOMEDATE
    """
    insert_sql = """
        insert into profit_hb_income (s_day, inland_ticket_incometype0, inland_ticket_incometype1,
        inland_ticket_incometype2, inter_ticket_income, inland_insure_income, inter_insure_income, createtime, updatetime)
        values (%s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        inland_ticket_incometype0 = VALUES(inland_ticket_incometype0),
        inland_ticket_incometype1 = VALUES(inland_ticket_incometype1),
        inland_ticket_incometype2 = VALUES(inland_ticket_incometype2),
        inter_ticket_income = VALUES(inter_ticket_income),
        inland_insure_income = VALUES(inland_insure_income),
        inter_insure_income = VALUES(inter_insure_income)
    """

    hb_profit = DBCli().sourcedb_cli.query_all(sql, [query_date, today])
    if hb_profit is None:
        return
    DBCli().targetdb_cli.batch_insert(insert_sql, hb_profit)
    pass


def update_profit_hotel_income(days=0):
    """更新酒店收入, profit_huoli_hotel_income"""
    query_start = DateUtil.get_date_before_days(days*7)
    query_end = DateUtil.get_date_after_days(1 - days)
    sql = """
        select date, total_profit from hotel_order_profit where date>=%s
        and date<%s
    """
    hotel_data = DBCli().tongji_skyhotel_cli.query_all(sql, [query_start, query_end])
    if hotel_data is None:
        return
    insert_sql = """
        insert into profit_huoli_hotel_income (s_day, income, createtime, updatetime)
        values (%s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        income = VALUES(income)
    """
    DBCli().targetdb_cli.batch_insert(insert_sql, hotel_data)
    pass


def update_operation_hbgj_channel_ticket_profit_daily(days=0):
    """更新航班各个渠道的利润, operation_hbgj_channel_ticket_profit_daily"""
    query_start = DateUtil.get_date_before_days(days*1)
    query_end = DateUtil.get_date_after_days(1 - days)

    income_sql = """
        select a.INCOMEDATE, b.SALETYPE, b.NAME, a.PNRSOURCE, SUM(INCOME),
        (select supplier_name
        from flow.sys_supplier
        where channeltype=1
        and supplier_name!='杰成'
        and supplier_id=TICKET_ORDER.agentid) agaent_name, TICKET_ORDER.agentid from TICKET_ORDER_INCOME a
        left join PNRSOURCE_CONFIG b ON a.PNRSOURCE = b.PNRSOURCE
        left join TICKET_ORDER ON a.ORDERID=TICKET_ORDER.ORDERID
        where a.INCOMEDATE >= %s and a.INCOMEDATE < %s
        and a.TYPE=0
        GROUP BY a.PNRSOURCE, a.INCOMEDATE, agaent_name, TICKET_ORDER.agentid order by a.INCOMEDATE
    """
    dto = [DateUtil.date2str(query_start, "%Y-%m-%d"), DateUtil.date2str(query_end, "%Y-%m-%d")]
    income_data = DBCli().sourcedb_cli.query_all(income_sql, dto)
    cost_sql = """
        select a.COSTDATE, P_C.SALETYPE,P_C.NAME, a.PNRSOURCE, SUM(AMOUNT) COST_AMOUNT,
        (select supplier_name
        from flow.sys_supplier
        where channeltype=1
        and supplier_name!='杰成'
        and supplier_id=TICKET_ORDER.agentid) agaent_name, TICKET_ORDER.agentid from TICKET_ORDER_COST a
        left join PNRSOURCE_CONFIG P_C ON a.PNRSOURCE=P_C.PNRSOURCE
        left join TICKET_ORDER ON a.ORDERID=TICKET_ORDER.ORDERID
        where a.COSTDATE >= %s and a.COSTDATE < %s
        and AMOUNTTYPE!=2
        GROUP BY a.PNRSOURCE, a.COSTDATE, agaent_name, TICKET_ORDER.agentid
    """
    cost_data = DBCli().sourcedb_cli.query_all(cost_sql, dto)

    cost_data_dict = {}

    for c_d in cost_data:
        cost_date, s_type, pn_name, pn_r, cost_mon, aga_name, aga_id = c_d
        new_cost = get_sale_type(s_type, pn_r, list(c_d))

        cost_key = new_cost[3]

        if cost_key == "hlth" and "hlth" in cost_data_dict:
            cost_data_dict["hlth"][4] += new_cost[4]
        else:
            if cost_key in cost_data_dict:
                (cost_data_dict[cost_key])[4] += new_cost[4]
            else:
                cost_data_dict[cost_key] = new_cost

    hlth_cost_sql = """
        SELECT
        case when
            DATE_FORMAT(od.CREATETIMe, '%%Y-%%m-%%d') >= '2017-09-01'
                then sum(od.REALPRICE +  od.AIRPORTFEE)*0.0018
            else sum(od.REALPRICE +  od.AIRPORTFEE)*0.005
        end as dft_amount,
        DATE_FORMAT(od.CREATETIMe, '%%Y-%%m-%%d') s_day
        FROM `TICKET_ORDERDETAIL` od
        INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        where od.CREATETIMe>=%s
        and od.CREATETIMe<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        AND IFNULL(od.`LINKTYPE`, 0) != 2
        and o.PNRSOURCE='hlth'
        and o.SUBORDERNO='BOP' and
        od.ETICKET is not null
        group by s_day
    """
    hlth_cost_data = DBCli().sourcedb_cli.query_all(hlth_cost_sql, dto)
    hlth_cost_data = hlth_cost_data[0]
    profit_data = []

    income_pn = []

    hlth_profit = {}

    profit_add_pid_data_dict = {}
    for income in income_data:

        saletype, pn_rsource = income[1], income[3]
        new_income_data = get_sale_type(saletype, pn_rsource, list(income))
        s_day, saletype, pn_name, pn_rsource, amount, pid = new_income_data
        income_pn.append(pn_rsource)

        if pn_rsource == "hlth" and "hlth" not in hlth_profit:
            hlth_profit["hlth"] = new_income_data
            hlth_profit["hlth"][4] -= float(cost_data_dict.get("hlth")[4])
            hlth_profit["hlth"][4] -= float(hlth_cost_data[0])
            continue
        elif pn_rsource == "hlth" and "hlth" in hlth_profit:
            hlth_profit["hlth"][4] += amount
            continue

        profit_key = pn_rsource
        if profit_key in profit_add_pid_data_dict:
            (profit_add_pid_data_dict[profit_key])[4] += amount
        else:
            cost_amount = cost_data_dict.get(pn_rsource, None)
            if cost_amount:
                cost_amount = cost_amount[4]
            else:
                cost_amount = 0

            profit_amount = amount - cost_amount
            new_income_data[4] = profit_amount
            profit_add_pid_data_dict[profit_key] = new_income_data

        # profit_data.append(new_income_data)

        profit_data = [pro_v for pro_k, pro_v in profit_add_pid_data_dict.items()]

    has_cost_no_income = set(cost_data_dict.keys()).difference(set(income_pn))
    has_cost_no_income = tuple(has_cost_no_income)

    if len(has_cost_no_income) > 0:
        for cost_no_income_key in has_cost_no_income:

            cost_data_dict[cost_no_income_key][4] = -float(cost_data_dict[cost_no_income_key][4])
            profit_data.append(cost_data_dict[cost_no_income_key])

    profit_data.append(hlth_profit["hlth"])
    insert_sql = """
        insert into operation_hbgj_channel_ticket_profit_daily (s_day, saletype, channel_name, pn_resouce,
        profit_amount, pid,
        createtime, updatetime) values (%s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        saletype = values(saletype),
        channel_name = values(channel_name),
        pn_resouce = values(pn_resouce),
        profit_amount = values(profit_amount),
        pid = values(pid)

    """
    DBCli().targetdb_cli.batch_insert(insert_sql, profit_data)
    pass


def get_sale_type(saletype, pn_resouce, new_channel_data):
    agaent_id = new_channel_data.pop()
    agaent_name = new_channel_data.pop()
    if saletype in (10, 11, 14):
        sale_type = 1
    elif saletype in (0, 12) and pn_resouce != 'supply' and pn_resouce != 'hlth':
        sale_type = 2
    elif saletype in (20, 21, 22) and pn_resouce != 'intsupply':
        sale_type = 3
    elif pn_resouce == 'intsupply':
        sale_type = 4
    elif saletype == 13 or pn_resouce == 'hlth' or saletype == 23:
        sale_type = 5
    elif pn_resouce == "supply" and (agaent_name is not None and len(agaent_name) > 0):
        new_channel_data[2] = agaent_name
        new_channel_data[3] = str(agaent_id)
        # print new_channel_data
        sale_type = 4
    else:
        sale_type = 2
    new_channel_data.append(sale_type)
    return new_channel_data


def update_profit_hb_income_official_website(days=0):
    """更新官网航班收入, profit_hb_income_official_website"""
    query_date = DateUtil.date2str(DateUtil.get_date_before_days(days*5), '%Y-%m-%d')
    today = DateUtil.date2str(DateUtil.get_date_after_days(1 - days), '%Y-%m-%d')
    sql = """
        SELECT create_date,CONCAT(module,'_',channel,'_',income_type) AS dataName, income_amount
        FROM booking.income_statics_daily
        where create_date >= %s and create_date< %s
        GROUP BY create_date,module,channel,income_type
        ORDER BY create_date ASC

    """
    insert_sql = """
        insert into profit_hb_income_official_website (s_day, income_name, income_amount, createtime, updatetime)
        values (%s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        income_name = VALUES(income_name),
        income_amount = VALUES(income_amount)
    """

    hb_profit = DBCli().sourcedb_cli.query_all(sql, [query_date, today])
    DBCli().targetdb_cli.batch_insert(insert_sql, hb_profit)
    pass

if __name__ == "__main__":
    # update_hb_car_hotel_profit(1)
    # update_profit_hb_income(1)
    # update_profit_hb_income_official_website(1)
    # i = 1
    # while i <= 41:
    #     update_huoli_car_income_daily(i)
    #     update_huoli_car_income_type(i)
    #     update_hb_car_hotel_profit(i)
    #     i += 1
    # update_hb_car_hotel_profit(1)
    # update_car_cost_detail(1)
    # i = 1
    # while i <= 5:
    #     update_operation_hbgj_channel_ticket_profit_daily(i)
    #     i += 1
    # i = 13
    # while i >= 1:
    #     update_operation_hbgj_channel_ticket_profit_daily(i)
    #     i -= 1
    update_hb_car_hotel_profit(1)