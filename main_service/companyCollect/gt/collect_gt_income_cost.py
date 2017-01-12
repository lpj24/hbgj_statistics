# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import datetime


def collect_income_cost():
    query_sql = """
        select *
        from income_and_cost
    """

    update_income_sql = """
        insert into profit_gt_income (s_day, type, amount, createtime, updatetime)
        values (%s, %s, %s, now(), now())
    """

    update_cost_sql = """
        insert into profit_gt_cost (s_day, type, amount, createtime, updatetime)
        values (%s, %s, %s, now(), now())
    """
    result_income_cost = DBCli(dict).gt_cli.queryAll(query_sql)

    income_cost_type = DBCli().gt_cli.queryAll("select COLUMN_NAME from information_schema.COLUMNS "
                                               "where table_name = 'income_and_cost'")
    income_cost_type = [i[0] for i in income_cost_type]
    income_cost_type.pop(0)
    income_cost_type.pop(-1)
    income_cost_type.pop(-1)
    # income_cost_type.reverse()
    for result in result_income_cost:
        s_day = result["s_date"]

        for i_c_type in income_cost_type:
            v = result[i_c_type] if result.get(i_c_type, None) else 0
            dto = [s_day, i_c_type, v]
            if i_c_type.startswith("in"):
                DBCli().targetdb_cli.insert(update_income_sql, dto)
            elif i_c_type.startswith("cost"):
                DBCli().targetdb_cli.insert(update_cost_sql, dto)

if __name__ == "__main__":
    collect_income_cost()