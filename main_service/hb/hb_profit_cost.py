# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import requests
import sys
reload(sys)
sys.setdefaultencoding('utf8')


def update_hb_car_hotel_profit(days=0):
    """航班专车酒店成本(德付通9.1日以前系数是0.005以后是0.0018),
    profit_hb_cost profit_huoli_car_cost profit_huoli_hotel_cost"""
    query_date = DateUtil.get_date_before_days(days * 7)
    today = DateUtil.get_date_after_days(1 - days)
    # sql = """
    #     select distinct TRADE_TIME s_day,
    #     sum(case when (AMOUNT_TYPE=2 and PRODUCT='0' and TRADE_CHANNEL not like '%%coupon%%') then amount else 0 end) paycost_in,
    #     sum(case when (AMOUNT_TYPE=3 and PRODUCT='0' and TRADE_CHANNEL not like '%%coupon%%') then amount else 0 end) paycost_return,
    #     sum(case when (AMOUNT_TYPE=1 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST)='0' and
    #     (PRODUCT=COST OR COST IS NULL) and TRADE_CHANNEL='coupon')
    #     then amount else 0 end) coupon_in,
    #     sum(case when (AMOUNT_TYPE=4 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST)='0' and
    #     (PRODUCT=COST OR COST IS NULL) and TRADE_CHANNEL='coupon')
    #     then amount else 0 end) coupon_return,
    #     sum(case when (AMOUNT_TYPE=1 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST)='0' and
    #     PRODUCT!=COST and TRADE_CHANNEL='coupon')
    #     then amount else 0 end) else_coupon_in,
    #     sum(case when (AMOUNT_TYPE=4 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST)='0' and
    #     PRODUCT!=COST and TRADE_CHANNEL='coupon')
    #     then amount else 0 end) else_coupon_return,
    #     sum(case when (AMOUNT_TYPE=6 and PRODUCT='20') then amount else 0 end) delay_care,
    #     sum(case when (AMOUNT_TYPE=5 and PRODUCT in ('1')) then amount else 0 end) point_give_amount,
    #     sum(case when (AMOUNT_TYPE=6 and PRODUCT in ('6','8','24','25')) then amount else 0 end) balance_give_amount
    #     from PAY_COST_INFO where TRADE_TIME>=%s and TRADE_TIME<%s
    #     group by TRADE_TIME
    # """
    sql = """
        select distinct TRADE_TIME s_day,
        sum(case when (AMOUNT_TYPE=2 and PRODUCT='0' and TRADE_CHANNEL not like '%%coupon%%') then amount else 0 end) paycost_in,
        sum(case when (AMOUNT_TYPE=3 and PRODUCT='0' and TRADE_CHANNEL not like '%%coupon%%') then amount else 0 end) paycost_return,
        sum(case when (AMOUNT_TYPE=1 and cost=1 and product=0 and TRADE_CHANNEL='coupon')
        then amount else 0 end) coupon_in,
        sum(case when (AMOUNT_TYPE=4 and cost=1 and product=0 and TRADE_CHANNEL='coupon')
        then amount else 0 end) coupon_return,
        sum(case when (AMOUNT_TYPE=1 and cost!=1 and cost!=10 and product=0 and TRADE_CHANNEL='coupon')
        then amount else 0 end) else_coupon_in,
        sum(case when (AMOUNT_TYPE=4 and cost!=1 and cost!=10 and product=0 and TRADE_CHANNEL='coupon')
        then amount else 0 end) else_coupon_return,
        sum(case when (AMOUNT_TYPE=6 and PRODUCT='20') then amount else 0 end) delay_care,
        sum(case when (AMOUNT_TYPE=5 and PRODUCT in ('1')) then amount else 0 end) point_give_amount,
        
        sum(case when (AMOUNT_TYPE=6 and PRODUCT ='8')  then amount else 0 end) balance_give_amount_8,
        sum(case when (AMOUNT_TYPE=6 and PRODUCT ='24')  then amount else 0 end) balance_give_amount_24,
        sum(case when (AMOUNT_TYPE=6 and PRODUCT ='42')  then amount else 0 end) balance_give_amount_42
        from PAY_COST_INFO where TRADE_TIME>=%s and TRADE_TIME<%s
        group by TRADE_TIME
    """
    dto = [query_date, today]
    result = DBCli().pay_cost_cli.query_all(sql, dto)
    other_cost_sql = """
        select
        sum(case when T_COST.INTFLAG=0 and o.mode=0 and INCOMETYPE= 0 then AMOUNT else 0 end) inland_price_diff_type0,
        sum(case when T_COST.INTFLAG=0 and o.mode=0 and INCOMETYPE= 1 then AMOUNT else 0 end) inland_price_diff_type1,
        sum(case when T_COST.INTFLAG=0 and o.mode=0 and INCOMETYPE= 2 then AMOUNT else 0 end) inland_price_diff_type2,
        sum(case when AMOUNTTYPE in (2, 3) then AMOUNT else 0 end) inland_refund_new,
        sum(case when T_COST.INTFLAG=1 and AMOUNTTYPE in (0, 1) then AMOUNT else 0 end) inter_price_diff,
        COSTDATE
        FROM TICKET_ORDER_COST T_COST
        left join TICKET_ORDER_INCOME_TYPE T_TYPE
        ON T_COST.PNRSOURCE = T_TYPE.PNRSOURCE
        left join skyhotel.`TICKET_ORDER` o
        on T_COST.ORDERID=o.ORDERID
        where COSTDATE>=%s and COSTDATE<%s
        GROUP BY COSTDATE
        ORDER BY COSTDATE
    """

    other_result = DBCli().sourcedb_cli.query_all(other_cost_sql, dto)
    update_other_cost_sql = """
        update profit_hb_cost set inland_price_diff_type0=%s, inland_price_diff_type1=%s,
        inland_price_diff_type2=%s, inland_refund_new=%s, 
        inter_price_diff=%s where s_day=%s
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

    query_dft_cost_fund_sql = """
        SELECT
        case when
            left(r.submit_time,10) >= '2017-09-01'
                then -sum(od.REALPRICE +  od.AIRPORTFEE)*0.0018
            else -sum(od.REALPRICE +  od.AIRPORTFEE)*0.005
        end as dft_amount,

        left(r.submit_time,10) s_day
               FROM `TICKET_ORDERDETAIL` od
        INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
               join TICKET_ORDER_REFUND r on od.refundid=r.orid 
        where r.submit_time>=%s
        and r.submit_time<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        AND IFNULL(od.`LINKTYPE`, 0) != 2
        and o.PNRSOURCE='hlth'
        and o.SUBORDERNO='BOP' and
        od.ETICKET is not null and  IFNULL(od.REFUNDID, 0) != 0
        group by left(r.submit_time,10);
    """

    update_dft_fund_cost_sql = """
        update profit_hb_cost set dft_cost_refund=%s where s_day=%s
    """

    dft_fund_cost = DBCli().sourcedb_cli.query_all(query_dft_cost_fund_sql, dto)

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
        else_coupon_in, else_coupon_return, delay_care, point_give_amount, balance_give_amount_8, 
        balance_give_amount_24, balance_give_amount_42, createtime, updatetime) values (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()
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
        balance_give_amount_8 = VALUES(balance_give_amount_8),
        balance_give_amount_24 = VALUES(balance_give_amount_24),
        balance_give_amount_42 = VALUES(balance_give_amount_42)
    """
    DBCli().targetdb_cli.batch_insert(insert_sql, result)
    DBCli().targetdb_cli.batch_insert(update_other_cost_sql, other_result)
    DBCli().targetdb_cli.batch_insert(update_dft_cost_sql, dft_result)
    DBCli().targetdb_cli.batch_insert(update_cut_point_cost_sql, cut_point_amount)
    DBCli().targetdb_cli.batch_insert(update_dft_fund_cost_sql, dft_fund_cost)

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
    """专车成本明细, profit_huoli_car_cost_type"""
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
    """伙力专车收入, profit_huoli_car_income"""
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
    """专车的收入类型, profit_huoli_car_income_type"""
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
    """航班收入, profit_hb_income"""
    query_date = DateUtil.get_date_before_days(days*15)
    today = DateUtil.get_date_after_days(1 - days)
    sql = """
        SELECT INCOMEDATE,
        SUM(case when TYPE=0 AND o.mode= 0 AND T_INCOME.INTFLAG=0 AND INCOMETYPE= 0 THEN INCOME else 0 END) inland_ticket_incometype0,
        SUM(case when TYPE=0 AND o.mode= 0 AND T_INCOME.INTFLAG=0 AND INCOMETYPE= 1 THEN INCOME else 0 END) inland_ticket_incometype1,
        SUM(case when TYPE=0 AND o.mode= 0 AND T_INCOME.INTFLAG=0 AND INCOMETYPE= 2 THEN INCOME else 0 END) inland_ticket_incometype2,
        sum(case when TYPE=0 AND T_INCOME.INTFLAG=1 THEN INCOME else 0 END) inter_ticket_income,
        sum(case when TYPE=1 AND T_INCOME.INTFLAG=0 THEN INCOME else 0 END) inland_insure_income,
        sum(case when TYPE=1 AND T_INCOME.INTFLAG=1 THEN INCOME else 0 END) inter_insure_income
        FROM TICKET_ORDER_INCOME T_INCOME
        left join TICKET_ORDER_INCOME_TYPE T_TYPE
        ON T_INCOME.PNRSOURCE = T_TYPE.PNRSOURCE
        left join skyhotel.`TICKET_ORDER` o
        on T_INCOME.ORDERID=o.ORDERID
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

    inter_inland_sql = """
        SELECT
        sum(case when INTFLAG=0 then total_price else 0 end) diff_nation,
        sum(case when INTFLAG=1 then total_price else 0 end) diff_inter,
        DATE_FORMAT(f.create_time,'%%Y-%%m-%%d') s_day
        FROM fcode.`activity_order` f
        join TICKET_ORDER o
        on f.ref_order_id=o.ORDERID
        WHERE pay_product_id=60
        and f.`status`=1
        and f.create_time >= %s
        and f.create_time < %s
        group by s_day
        order by s_day
    """

    update_inter_inland_sql = """
        update profit_hb_income set diff_nation = %s, diff_inter=%s, updatetime=now()
        where s_day=%s
    """

    hb_profit = DBCli().sourcedb_cli.query_all(sql, [query_date, today])
    if hb_profit is None:
        return
    DBCli().targetdb_cli.batch_insert(insert_sql, hb_profit)
    inter_inland_data = DBCli().sourcedb_cli.query_all(inter_inland_sql, [query_date, today])
    DBCli().targetdb_cli.batch_insert(update_inter_inland_sql, inter_inland_data)

    official_insure_income_sql = """
        SELECT 
        sum(pay_price+outpay_price*r.RATE-outpay_price) ,
        left(o.createtime,10)
        FROM `TICKET_ORDER_ITEMDETAIL` o 
        join TICKET_INSURE_INCOME_RULE r 
        on r.insureid=SUBSTR(extinfo,10,8) 
        WHERE item_id<>'7' 
        and realitem_id='2' 
        and left(o.createtime,10) >= %s
        and left(o.createtime,10) < %s
        and `status`=2  
        and pay_price<>1 GROUP BY left(o.createtime,10)
    """
    update_office_sql = """
        update profit_hb_income set official_insure_income = %s, updatetime=now()
        where s_day=%s
    """

    office_data = DBCli().sourcedb_cli.query_all(official_insure_income_sql, [query_date, today])
    DBCli().targetdb_cli.batch_insert(update_office_sql, office_data)

    no_website_sql = """
        SELECT 
        sum(pay_price+outpay_price*r.RATE-outpay_price) insure_package,
        left(o.createtime,10) s_day
        FROM `TICKET_ORDER_ITEMDETAIL` o join TICKET_INSURE_INCOME_RULE r 
        on r.insureid=SUBSTR(extinfo,10,8) 
        WHERE item_id='7' 
        and realitem_id='2' and `status`=2  
        and left(o.createtime,10)>=%s 
        and left(o.createtime,10)<%s
        and pay_price<>1 GROUP BY left(o.createtime,10);
    """

    update_no_office_sql = """
        update profit_hb_income set insure_package = %s, updatetime=now()
        where s_day=%s
    """
    no_office_data = DBCli().sourcedb_cli.query_all(no_website_sql, [query_date, today])
    DBCli().targetdb_cli.batch_insert(update_no_office_sql, no_office_data)


def update_profit_hotel_income(days=0):
    """酒店收入, profit_huoli_hotel_income"""
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
    """航班各个渠道的利润, operation_hbgj_channel_ticket_profit_daily"""
    query_start = DateUtil.get_date_before_days(days*1)
    query_end = DateUtil.get_date_after_days(1 - days)

    supplier_sql = """
        select concat(supplier_id), supplier_name
        from supplier.sys_supplier
        where channeltype=1
        and supplier_name!='杰成'
    """

    supplier_data = dict(DBCli().targetdb_cli.query_all(supplier_sql))

    income_sql = """
        select a.INCOMEDATE, b.SALETYPE, b.NAME, a.PNRSOURCE, SUM(INCOME),
        TICKET_ORDER.agentid from TICKET_ORDER_INCOME a
        left join PNRSOURCE_CONFIG b ON a.PNRSOURCE = b.PNRSOURCE
        left join TICKET_ORDER ON a.ORDERID=TICKET_ORDER.ORDERID
        where a.INCOMEDATE >= %s and a.INCOMEDATE < %s
        and a.TYPE=0
        GROUP BY a.PNRSOURCE, a.INCOMEDATE, TICKET_ORDER.agentid order by a.INCOMEDATE
    """
    dto = [DateUtil.date2str(query_start, "%Y-%m-%d"), DateUtil.date2str(query_end, "%Y-%m-%d")]
    income_data = DBCli().sourcedb_cli.query_all(income_sql, dto)
    cost_sql = """
        select a.COSTDATE, P_C.SALETYPE,P_C.NAME, a.PNRSOURCE, SUM(AMOUNT) COST_AMOUNT,
        TICKET_ORDER.agentid from TICKET_ORDER_COST a
        left join PNRSOURCE_CONFIG P_C ON a.PNRSOURCE=P_C.PNRSOURCE
        left join TICKET_ORDER ON a.ORDERID=TICKET_ORDER.ORDERID
        where a.COSTDATE >= %s and a.COSTDATE < %s
        and AMOUNTTYPE!=2
        GROUP BY a.PNRSOURCE, a.COSTDATE, TICKET_ORDER.agentid
    """
    cost_data = DBCli().sourcedb_cli.query_all(cost_sql, dto)

    cost_data_dict = {}

    for c_d in cost_data:
        new_cd = list(c_d)
        new_cd.insert(-1, supplier_data.get(new_cd[-1], None))
        cost_date, s_type, pn_name, pn_r, cost_mon, aga_name, aga_id = new_cd
        new_cost = get_sale_type(s_type, pn_r, new_cd)

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
        new_cd = list(income)
        # if new_cd[-1] == 677:
        #     print supplier_data.get(new_cd[-1], None)
        new_cd.insert(-1, supplier_data.get(new_cd[-1], None))
        saletype, pn_rsource = new_cd[1], new_cd[3]
        # print new_cd
        new_income_data = get_sale_type(saletype, pn_rsource, new_cd)
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
    """官网航班收入, profit_hb_income_official_website"""
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


def update_hb_inter_coupon_cost_daily(days=0):
    """国外航班优惠券支付成本, profit_hb_inter_cost"""
    query_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 3), '%Y-%m-%d')
    today = DateUtil.date2str(DateUtil.get_date_after_days(1 - days), '%Y-%m-%d')
    coupon_sql = """
        select distinct
        sum(case when (AMOUNT_TYPE=1 and cost=10 and TRADE_CHANNEL='coupon')
        then amount else 0 end) coupon_in,
        sum(case when (AMOUNT_TYPE=4 and cost=10 and TRADE_CHANNEL='coupon')
        then amount else 0 end) coupon_return,
        TRADE_TIME s_day
        from PAY_COST_INFO where TRADE_TIME>=%s and TRADE_TIME<%s
        group by TRADE_TIME
    """

    pay_cost_in_sql = """
        SELECT DATE_FORMAT(ptr.create_time,'%%Y-%%m-%%d') s_day, sum(ptr.price*4/1000) pay_price1
        from pay_trade_record ptr,TICKET_ORDER tico,PNRSOURCE_CONFIG pc
        where left(ptr.create_time,10)>=%s
        and left(ptr.create_time,10)<%s
        and ptr.PAYSOURCE in('weixinpay','bankcard','alipay','applepay','xinyf')
        and ptr.order_id=tico.ORDERID
        and tico.INTFLAG = 1
        and tico.PnrSource=pc.PnrSource
        group by s_day
        order by s_day;
    """

    pay_cost_in_other_sql = """
        SELECT DATE_FORMAT(eo.create_time,'%%Y-%%m-%%d') s_day,
        sum((ticod.price+ticod.ratefee)*4/1000) pay_price2
        from TICKET_ORDER tico,TICKET_ORDERDETAIL ticod,event_order eo,PNRSOURCE_CONFIG pc
        where tico.ORDERID=ticod.ORDERID
        and ticod.ORDERID = eo.order_id
        and eo.EVENT_group='519sec'
        and tico.INTFLAG=1 and tico.PNRSOURCE=pc.PNRSOURCE
        and left(eo.create_time,10)>=%s
        and left(eo.create_time,10)<%s
        group by s_day
        order by s_day;
    """

    insert_pay_cost_in_sql = """
        insert into profit_hb_inter_cost (s_day, paycost_in, paycost_return, coupon_in, coupon_return,
        createtime, updatetime)
        values
        (%s, %s, 0, 0, 0, now(), now())
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        paycost_in = VALUES(paycost_in),
        paycost_return = VALUES(paycost_return),
        coupon_in = VALUES(coupon_in),
        coupon_return = VALUES(coupon_return)
    """

    update_coupon_inter_sql = """
        update profit_hb_inter_cost set coupon_in = %s, coupon_return=%s
        where s_day=%s
    """

    dto = [query_date, today]
    coupon_inter = DBCli().pay_cost_cli.query_all(coupon_sql, dto)

    pay_cost_in = DBCli().sourcedb_cli.query_all(pay_cost_in_sql, dto)
    pay_cost_in_other = dict(DBCli().sourcedb_cli.query_all(pay_cost_in_other_sql, dto))

    pay_cost_in_all = []

    for key, val in dict(pay_cost_in).items():
        pay_cost_in_all.append([key, float(val) + pay_cost_in_other.get(key, 0)])

    DBCli().targetdb_cli.batch_insert(insert_pay_cost_in_sql, pay_cost_in_all)

    DBCli().targetdb_cli.batch_insert(update_coupon_inter_sql, coupon_inter)

    balance_give_amount_sql = """
        SELECT 
        sum(amount) balance_give_amount,
        left(rr.RECHARGE_TIME,10) sday
        from TICKET_ORDER tico,
        RECHARGE_RECORD rr
        where tico.ORDERID = rr.orderid 
        and tico.INTFLAG=1 
        and rr.RECHARGE_TIME>=%s
        and rr.RECHARGE_TIME<%s
        GROUP BY sday
    """

    update_balance_give_amount_sql = """
        update profit_hb_inter_cost set balance_give_amount = %s
        where s_day=%s
    """

    balance_give_amount_data = DBCli().sourcedb_cli.query_all(balance_give_amount_sql, dto)
    DBCli().targetdb_cli.batch_insert(update_balance_give_amount_sql, balance_give_amount_data)

    point_give_amount_sql = """
        SELECT 
        sum(upd.POINTS/1000) point_give_amount,
        left(upd.create_time,10) sday
        from TICKET_ORDER tico,
        USER_POINTS_DETAIL upd,
        USER_POINTS up
        where tico.ORDERID = upd.ORDER_ID 
        and tico.INTFLAG=1 
        and upd.TYPE=1 
        and upd.POINT_ID=up.ID 
        and up.TYPE=1 
        and upd.create_time>=%s
        and upd.create_time<%s
        GROUP BY sday
    """

    update_point_give_amount_sql = """
        update profit_hb_inter_cost set point_give_amount = %s
        where s_day=%s
    """

    point_give_amount_data = DBCli().sourcedb_cli.query_all(point_give_amount_sql, dto)
    DBCli().targetdb_cli.batch_insert(update_point_give_amount_sql, point_give_amount_data)


if __name__ == "__main__":
    update_profit_hb_income(1)
    # i = 1
    # while i <= 11:
    #     update_huoli_car_income_type(i)
    #     i += 1
    # i = 1
    # while i <= 352:
    #     update_operation_hbgj_channel_ticket_profit_daily(i)
    #     i += 1