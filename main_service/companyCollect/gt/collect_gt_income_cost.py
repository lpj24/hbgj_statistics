# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import datetime


def collect_income_cost():
    collect_mysql_columns = """
        select COLUMN_NAME from information_schema.COLUMNS where table_name = 'income_and_cost';
    """

    # mysql_columns = DBCli().gt_cli.queryAll(collect_mysql_columns)
    # in_come_column = []
    # in_cost_column = []
    # print mysql_columns
    # for column in mysql_columns:
    #     if column[0].startswith("in"):
    #         in_come_column.append(column[0])
    #     elif column[0].startswith("cost"):
    #         in_cost_column.append(column[0])
    #
    # start_date = datetime.date(2016, 12, 1)
    # end_date = datetime.date(2017, 1, 10)

    sql = """
        select * from income_and_cost
    """

    # insert_income_sql = """
    #     insert into profit_gt_income (s_day, type, amount) values (%s, %s, %s)
    # """

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