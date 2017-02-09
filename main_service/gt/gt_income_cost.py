# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_gt_income_cost(days):
    start_date = DateUtil.get_date_before_days(7 * int(days))
    end_date = DateUtil.get_date_before_days(0)

    query_sql = """
        select *
        from income_and_cost where s_date >= %s and s_date < %s
    """

    insert_income_sql = """
        insert into profit_gt_income (s_day, type, amount, createtime, updatetime)
        values (%s, %s, %s, now(), now())
    """

    insert_cost_sql = """
        insert into profit_gt_cost (s_day, type, amount, createtime, updatetime)
        values (%s, %s, %s, now(), now())
    """

    update_income_sql = """
        update profit_gt_income set amount=%s, updatetime=now() where s_day = %s and type=%s
    """

    update_cost_sql = """
        update profit_gt_cost set amount=%s, updatetime=now() where s_day = %s and type=%s
    """

    query_dto = [start_date, end_date]
    result_income_cost = DBCli(dict).gt_cli.queryAll(query_sql, query_dto)

    income_cost_type = DBCli().gt_cli.queryAll("select COLUMN_NAME from information_schema.COLUMNS "
                                               "where table_name = 'income_and_cost'")
    income_cost_type = [i[0] for i in income_cost_type]
    income_cost_type.pop(0)
    income_cost_type.pop(-1)
    income_cost_type.pop(-1)
    # income_cost_type.reverse()
    for result in result_income_cost:
        s_day = result["s_date"]
        exists_income = DBCli().targetdb_cli.queryOne("select * from profit_gt_income where s_day=%s", [s_day])
        exists_cost = DBCli().targetdb_cli.queryOne("select * from profit_gt_cost where s_day=%s", [s_day])

        for i_c_type in income_cost_type:
            v = result[i_c_type] if result.get(i_c_type, None) else 0
            dto = [s_day, i_c_type, v]
            if i_c_type.startswith("in"):
                if not exists_income:
                    DBCli().targetdb_cli.insert(insert_income_sql, dto)
                else:
                    dto = [v, s_day, i_c_type]
                    DBCli().targetdb_cli.insert(update_income_sql, dto)
            elif i_c_type.startswith("cost"):
                if not exists_cost:
                    DBCli().targetdb_cli.insert(insert_cost_sql, dto)
                else:
                    dto = [v, s_day, i_c_type]
                    DBCli().targetdb_cli.insert(update_cost_sql, dto)

if __name__ == "__main__":
    update_gt_income_cost(1)