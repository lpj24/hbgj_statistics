# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_gt_income_cost(days):
    start_date = DateUtil.get_date_before_days(3 * int(days))
    end_date = DateUtil.get_date_before_days(0)

    query_sql = """
        select * from income_and_cost where s_date >= %s and s_date < %s
    """

    update_income_sql = """
        insert into profit_gt_income (s_day, type, amount, createtime, updatetime)
        values (%s, %s, %s, now(), now())
    """

    update_cost_sql = """
        insert into profit_gt_cost (s_day, type, amount, createtime, updatetime)
        values (%s, %s, %s, now(), now())
    """
    query_dto = [start_date, end_date]
    result_income_cost = DBCli(dict).gt_cli.queryAll(query_sql, query_dto)
    for result in result_income_cost:
        s_day = result["s_date"]
        exists_income = DBCli().targetdb_cli.queryOne("select * from profit_gt_income where s_day=%s", [s_day])
        exists_cost = DBCli().targetdb_cli.queryOne("select * from profit_gt_cost where s_day=%s", [s_day])
        for k, v in result.items():
            if k.startswith("in") and not exists_income:
                if not v:
                    v = 0
                dto = [s_day, k, v]
                DBCli().targetdb_cli.insert(update_income_sql, dto)
            elif k.startswith("cost") and not exists_cost:
                if not v:
                    v = 0
                dto = [s_day, k, v]
                DBCli().targetdb_cli.insert(update_cost_sql, dto)

if __name__ == "__main__":
    update_gt_income_cost(1)