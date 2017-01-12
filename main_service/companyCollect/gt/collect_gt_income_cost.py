# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import datetime


def collect_income_cost():
    sql = """
        select * from income_and_cost
    """

    insert_income_sql = """
        insert into profit_gt_income (s_day, type, amount) values (%s, %s, %s)
    """

    insert_cost_sql = """
        insert into profit_gt_cost (s_day, type, amount) values (%s, %s, %s)
    """
    result_income_cost = DBCli(dict).gt_cli.queryAll(sql)

    for result in result_income_cost:
        s_day = result["s_date"]
        for k, v in result.items():
            if k.startswith("in"):
                if not v:
                    v = 0
                dto = [s_day, k, v]
                DBCli().targetdb_cli.insert(insert_income_sql, dto)
            elif k.startswith("cost"):
                if not v:
                    v = 0
                dto = [s_day, k, v]
                DBCli().targetdb_cli.insert(insert_cost_sql, dto)

if __name__ == "__main__":
    collect_income_cost()