# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_hb_car_hotel_profit(days=0):
    query_date = DateUtil.get_date_before_days(days * 15)
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


def update_profit_hb_income(days=0):
    query_date = DateUtil.get_date_before_days(days)
    today = DateUtil.get_date_after_days(1 - days)
    sql = """
        SELECT INCOMEDATE,
        sum(case when TYPE=0 AND INTFLAG=0 THEN INCOME else 0 END) inland_ticket_income,
        sum(case when TYPE=0 AND INTFLAG=1 THEN INCOME else 0 END) inter_ticket_income,
        sum(case when TYPE=1 AND INTFLAG=0 THEN INCOME else 0 END) inland_insure_income,
        sum(case when TYPE=1 AND INTFLAG=1 THEN INCOME else 0 END) inter_insure_income
        FROM TICKET_ORDER_INCOME
        WHERE INCOMEDATE>=%s
        and INCOMEDATE<%s
        GROUP BY INCOMEDATE
        ORDER BY INCOMEDATE
    """
    insert_sql = """
        insert into profit_hb_income (s_day, inland_ticket_income, inter_ticket_income,
        inland_insure_income, inter_insure_income, createtime, updatetime)
        values (%s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        inland_ticket_income = VALUES(inland_ticket_income),
        inter_ticket_income = VALUES(inter_ticket_income),
        inland_insure_income = VALUES(inland_insure_income),
        inter_insure_income = VALUES(inter_insure_income)
    """

    hb_profit = DBCli().sourcedb_cli.queryOne(sql, [query_date, today])
    DBCli().targetdb_cli.insert(insert_sql, hb_profit)

if __name__ == "__main__":
    # update_hb_car_hotel_profit(1)
    # update_huoli_car_income_daily(1)
    # update_hb_car_hotel_profit(1)
    # i = 20
    # while i >= 1:
    #     print i
    #     update_huoli_car_income_daily(i)
    #     i -= 1
    update_profit_hb_income(1)