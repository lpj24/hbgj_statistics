# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from monitor_sql import sql, week_sql
from dbClient import utils
from main_service.gt import gt_income_cost


def check_day_data():
    query_date = DateUtil.get_date_before_days(1)
    query_date = DateUtil.date2str(query_date, '%Y-%m-%d')
    msg = ""
    for execute_sql in sql:
        dto = [query_date]
        data = DBCli().targetdb_cli.queryOne(execute_sql, dto)

        if data[0] < 1:
            #error
            msg += execute_sql.split(" ")[3] + "\n"
        else:
            pass
    if len(msg) > 0:
        utils.sendMail("lipenju24@163.com", msg, "数据查询异常")


def check_week_data():
    start_week, end_week = DateUtil.get_last_week_date()
    query_date = DateUtil.date2str(start_week, '%Y-%m-%d')

    msg = ""
    for execute_sql in week_sql:
        dto = [query_date]
        data = DBCli().targetdb_cli.queryOne(execute_sql, dto)

        if data[0] < 1:
            # error
            msg += execute_sql.split(" ")[3] + "\n"
        else:
            pass
    if len(msg) > 0:
        utils.sendMail("lipenju24@163.com", msg, "数据查询异常")


def update_gt_cost_income():
    gt_income_cost.update_gt_income_cost(1)

if __name__ == "__main__":
    # update_gt_cost_income()
    check_day_data()