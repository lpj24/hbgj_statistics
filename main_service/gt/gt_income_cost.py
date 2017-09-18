# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_gt_income_cost(days=0):
    """更新高铁收入与成本, profit_gt_income profit_gt_cost"""
    if days > 0:
        start_date = DateUtil.get_date_before_days(20 * int(days))
    else:
        start_date = DateUtil.get_date_before_days(1)
    end_date = DateUtil.get_date_before_days(0)

    query_sql = """
        select *
        from income_and_cost where s_date >= %s and s_date < %s
    """

    insert_income_sql = """
        insert into profit_gt_income (s_day, type, amount, createtime, updatetime)
        values (%s, %s, %s, now(), now())
        on duplicate key update updatetime = now() ,
        s_day = values(s_day),
        type = values(type),
        amount = values(amount)
    """

    insert_cost_sql = """
        insert into profit_gt_cost (s_day, type, amount, createtime, updatetime)
        values (%s, %s, %s, now(), now())
        on duplicate key update updatetime = now() ,
        s_day = values(s_day),
        type = values(type),
        amount = values(amount)
    """

    gt_coupon_use_sql = """
        select distinct TRADE_TIME s_day,
        sum(case when (AMOUNT_TYPE =1 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST)='29'
        and TRADE_CHANNEL='coupon') then amount else 0 end) coupon_in,
        sum(case when (AMOUNT_TYPE =4 and IF(ISNULL(COST) || LENGTH(trim(COST))<1, PRODUCT, COST)='29'
        and TRADE_CHANNEL='coupon') then amount else 0 end) coupon_return
        from PAY_COST_INFO where TRADE_TIME>=%s and TRADE_TIME<%s
        group by TRADE_TIME
    """

    query_dto = [start_date, end_date]

    result_income_cost = DBCli(dict).gt_cli.queryAll(query_sql, query_dto)

    income_cost_type = DBCli().gt_cli.queryAll("select COLUMN_NAME from information_schema.COLUMNS "
                                               "where table_name = 'income_and_cost'")
    income_cost_type = [i[0] for i in income_cost_type]
    income_cost_type.pop(0)
    income_cost_type.pop(-1)
    income_cost_type.pop(-1)

    for result in result_income_cost:
        s_day = result["s_date"]
        for i_c_type in income_cost_type:
            v = result[i_c_type] if result.get(i_c_type, None) else 0

            dto = [s_day, i_c_type, v]
            if i_c_type.startswith("in"):
                DBCli().targetdb_cli.insert(insert_income_sql, dto)
            elif i_c_type.startswith("cost"):
                DBCli().targetdb_cli.insert(insert_cost_sql, dto)
    gt_coupon_data = DBCli(dict).pay_cost_cli.queryAll(gt_coupon_use_sql, query_dto)
    insert_gt_coupon = []
    for gt_coupon in gt_coupon_data:
        coupon_in_return = gt_coupon["coupon_in"] - gt_coupon["coupon_return"]
        insert_gt_coupon.append([gt_coupon["s_day"], "coupon_in_return", coupon_in_return])

    DBCli().targetdb_cli.batchInsert(insert_cost_sql, insert_gt_coupon)

if __name__ == "__main__":
    update_gt_income_cost(1)