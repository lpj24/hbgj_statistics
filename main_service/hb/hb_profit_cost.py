# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import requests


def update_hb_car_hotel_profit(days=0):
    query_date = DateUtil.get_date_before_days(days * 7)
    today = DateUtil.get_date_after_days(1 - days)
    sql = """
        select distinct TRADE_TIME s_day,
        sum(case when (AMOUNT_TYPE=2 and PRODUCT='0' and TRADE_CHANNEL not like '%%coupon%%') then amount else 0 end) paycost_in,
        sum(case when (AMOUNT_TYPE=3 and PRODUCT='0' and TRADE_CHANNEL not like '%%coupon%%') then amount else 0 end) paycost_return,
        sum(case when (AMOUNT_TYPE=1 and PRODUCT='0' and TRADE_CHANNEL like '%%coupon%%') then amount else 0 end) coupon_in,
        sum(case when (AMOUNT_TYPE=4 and PRODUCT='0' and TRADE_CHANNEL like '%%coupon%%') then amount else 0 end) coupon_return,
        sum(case when (AMOUNT_TYPE=6 and PRODUCT='20') then amount else 0 end) delay_care,
        sum(case when (AMOUNT_TYPE=5 and PRODUCT in ('1')) then amount else 0 end) point_give_amount,
        sum(case when (AMOUNT_TYPE=6 and PRODUCT in ('6','8','24','25')) then amount else 0 end) balance_give_amount
        from PAY_COST_INFO where TRADE_TIME>=%s and TRADE_TIME<%s
        group by TRADE_TIME
    """
    dto = [query_date, today]
    result = DBCli().pay_cost_cli.queryAll(sql, dto)
    other_cost_sql = """
        select
        sum(case when T_COST.INTFLAG=0 and AMOUNTTYPE!=2 and INCOMETYPE= 0 then AMOUNT else 0 end) inland_price_diff_type0,
        sum(case when T_COST.INTFLAG=0 and AMOUNTTYPE!=2 and INCOMETYPE= 1 then AMOUNT else 0 end) inland_price_diff_type1,
        sum(case when T_COST.INTFLAG=0 and AMOUNTTYPE!=2 and INCOMETYPE= 2 then AMOUNT else 0 end) inland_price_diff_type2,
        sum(case when AMOUNTTYPE=2 then AMOUNT else 0 end) inland_refund_new,
        sum(case when T_COST.INTFLAG=1 and AMOUNTTYPE!=2 then AMOUNT else 0 end) inter_price_diff,
        COSTDATE
        FROM TICKET_ORDER_COST T_COST
        left join TICKET_ORDER_INCOME_TYPE T_TYPE
        ON T_COST.PNRSOURCE = T_TYPE.PNRSOURCE
        where COSTDATE>=%s and COSTDATE<%s
        GROUP BY COSTDATE
        ORDER BY COSTDATE
    """

    other_result = DBCli().sourcedb_cli.queryAll(other_cost_sql, dto)

    update_other_cost_sql = """
        update profit_hb_cost set inland_price_diff_type0=%s, inland_price_diff_type1=%s, inland_price_diff_type2=%s,
        inland_refund_new=%s, inter_price_diff=%s where s_day=%s
    """

    query_dft_cost_sql = """
        SELECT
        sum(od.REALPRICE +  od.AIRPORTFEE)*0.005 as dft_amount,
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
    dft_result = DBCli().sourcedb_cli.queryAll(query_dft_cost_sql, dto)

    insert_sql = """
        insert into profit_hb_cost (s_day, paycost_in, paycost_return, coupon_in, coupon_return,
        delay_care, point_give_amount, balance_give_amount, createtime, updatetime) values (
            %s, %s, %s, %s, %s, %s, %s, %s, now(), now()
        )
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        paycost_in = VALUES(paycost_in),
        paycost_return = VALUES(paycost_return),
        coupon_in = VALUES(coupon_in),
        coupon_return = VALUES(coupon_return),
        delay_care = VALUES(delay_care),
        point_give_amount = VALUES(point_give_amount),
        balance_give_amount = VALUES(balance_give_amount)
    """
    DBCli().targetdb_cli.batchInsert(insert_sql, result)
    DBCli().targetdb_cli.batchInsert(update_other_cost_sql, other_result)
    DBCli().targetdb_cli.batchInsert(update_dft_cost_sql, dft_result)

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

    result = DBCli().pay_cost_cli.queryAll(car_sql, dto)
    insert_car_sql = """
        insert into profit_huoli_car_cost (s_day, paycost_in, paycost_return, coupon_in, coupon_return,
         point_give_amount, balance_give_amount, createtime, updatetime) values (
            %s, %s, %s, %s, %s, %s, %s, now(), now()
        )
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        paycost_in = VALUES(paycost_in),
        paycost_return = VALUES(paycost_return),
        coupon_in = VALUES(coupon_in),
        coupon_return = VALUES(coupon_return),
        point_give_amount = VALUES(point_give_amount),
        balance_give_amount = VALUES(balance_give_amount)
    """
    DBCli().targetdb_cli.batchInsert(insert_car_sql, result)

    hotel_sql = """
        select distinct TRADE_TIME s_day,
        sum(case when (AMOUNT_TYPE=2 and PRODUCT='36' and TRADE_CHANNEL not like '%%coupon%%') then amount else 0 end) paycost_in,
        sum(case when (AMOUNT_TYPE=3 and PRODUCT='36' and TRADE_CHANNEL not like '%%coupon%%') then amount else 0 end) paycost_return,
        sum(case when (AMOUNT_TYPE=1 and PRODUCT='36' and TRADE_CHANNEL like '%%coupon%%') then amount else 0 end) coupon_in,
        sum(case when (AMOUNT_TYPE=4 and PRODUCT='36' and TRADE_CHANNEL like '%%coupon%%') then amount else 0 end) coupon_return,
        sum(case when (AMOUNT_TYPE=5 and PRODUCT in ('8')) then amount else 0 end) point_give_amount,
        sum(case when (AMOUNT_TYPE=6 and PRODUCT in ('9','10')) then amount else 0 end) balance_give_amount
        from PAY_COST_INFO where TRADE_TIME>=%s and TRADE_TIME<%s
        group by s_day
    """

    result = DBCli().pay_cost_cli.queryAll(hotel_sql, dto)
    insert_hotel_sql = """
        insert into profit_huoli_hotel_cost (s_day, paycost_in, paycost_return, coupon_in, coupon_return,
         point_give_amount, balance_give_amount, createtime, updatetime) values (
            %s, %s, %s, %s, %s, %s, %s, now(), now()
        )
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        paycost_in = VALUES(paycost_in),
        paycost_return = VALUES(paycost_return),
        coupon_in = VALUES(coupon_in),
        coupon_return = VALUES(coupon_return),
        point_give_amount = VALUES(point_give_amount),
        balance_give_amount = VALUES(balance_give_amount)
    """
    DBCli().targetdb_cli.batchInsert(insert_hotel_sql, result)


def update_car_cost_detail(days=0):
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
    result = DBCli(dict).pay_cost_cli.queryAll(car_sql, dto)
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
        url = "http://58.83.139.232:8070/mall/bi/costdetail"
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
    DBCli().targetdb_cli.batchInsert(insert_sql, insert_car_cost)


def update_huoli_car_income_daily(days=0):
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
    url = "http://58.83.139.232:8070/mall/bi/income"
    params = {"beginDate": DateUtil.date2str(query_date, '%Y-%m-%d'), "endDate": DateUtil.date2str(today, '%Y-%m-%d')}
    car_result = requests.get(url, params=params).json()
    car_result = car_result["result"][0]
    DBCli().targetdb_cli.insert(insert_car_sql, [car_result['date'], car_result['income']])


def update_huoli_car_income_type(days=0):
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
    url = "http://58.83.139.232:8070/mall/bi/incomedetail"
    params = {"beginDate": DateUtil.date2str(query_date, '%Y-%m-%d'), "endDate": DateUtil.date2str(today, '%Y-%m-%d')}
    car_result = requests.get(url, params=params).json()
    car_result = car_result["result"]
    insert_car_income = []
    for car_income_data in car_result:
        car_date = car_income_data["date"]
        income_type = car_income_data["type"]
        income_amount = car_income_data["amount"]
        insert_car_income.append((car_date, income_type, income_amount))
    DBCli().targetdb_cli.batchInsert(insert_car_sql, insert_car_income)


def update_profit_hb_income(days=0):
    query_date = DateUtil.get_date_before_days(days)
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

    hb_profit = DBCli().sourcedb_cli.queryOne(sql, [query_date, today])
    if hb_profit is None:
        return
    DBCli().targetdb_cli.insert(insert_sql, hb_profit)


def update_profit_hotel_income(days=0):
    query_start = DateUtil.get_date_before_days(days*7)
    query_end = DateUtil.get_date_after_days(1 - days)
    sql = """
        select date, total_profit from hotel_order_profit where date>=%s
        and date<%s
    """
    hotel_data = DBCli().tongji_skyhotel_cli.queryAll(sql, [query_start, query_end])
    if hotel_data is None:
        return
    insert_sql = """
        insert into profit_huoli_hotel_income (s_day, income, createtime, updatetime)
        values (%s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        income = VALUES(income)
    """
    DBCli().targetdb_cli.batchInsert(insert_sql, hotel_data)

if __name__ == "__main__":
    update_profit_hotel_income(1)