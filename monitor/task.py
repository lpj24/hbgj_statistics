# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from monitor_sql import sql, week_sql
from dbClient import utils
from main_service.gt import gt_income_cost
from main_service.hb import hb_profit_cost


def check_day_data():
    query_date = DateUtil.get_date_before_days(1)
    query_date = DateUtil.date2str(query_date, '%Y-%m-%d')
    msg = ""
    for execute_sql in sql:
        dto = [query_date]
        data = DBCli().targetdb_cli.queryOne(execute_sql, dto)

        if data[0] < 1:
            #error
            msg += execute_sql.split(" ")[3] + "<br/>"
        else:
            pass
    if len(msg) > 0:
        utils.sendMail("lipenju24@163.com", msg, "数据查询异常")
    else:
        utils.sendMail("lipenju24@163.com", "数据查询正常", "数据查询正常")


def check_week_data():
    start_week, end_week = DateUtil.get_last_week_date()
    query_date = DateUtil.date2str(start_week, '%Y-%m-%d')

    msg = ""
    for execute_sql in week_sql:
        dto = [query_date]
        data = DBCli().targetdb_cli.queryOne(execute_sql, dto)

        if data[0] < 1:
            # error
            msg += execute_sql.split(" ")[3] + "<br/>"
        else:
            pass
    if len(msg) > 0:
        utils.sendMail("lipenju24@163.com", msg, "周数据查询异常")
    else:
        utils.sendMail("lipenju24@163.com", "周数据查询正常", "周数据查询正常")


def execute_later_job():
    # gt_income_cost.update_gt_income_cost(1)
    # hb_profit_cost.update_hb_car_hotel_profit(1)
    # hb_profit_cost.update_car_cost_detail(1)
    # hb_profit_cost.update_huoli_car_income_type(1)
    hb_profit_cost.update_profit_hotel_income(1)


def check_execute_job():
    from time_job_excute import excute_day, excute_mon_week, excute_month
    day_service = excute_day.add_execute_job()
    week_service = excute_mon_week.add_execute_job()
    month_service = excute_month.add_execute_job()
    for job in day_service.get_day_service():
        print job

    for week_job in week_service.get_week_mon_service():
        print week_job

    for week_job in month_service.get_month_first_service():
        print week_job

if __name__ == "__main__":
    # execute_later_job()
    check_day_data()
