# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from monitor_sql import sql, week_sql
from dbClient import utils
from main_service.gt import gt_income_cost
from main_service.hb import hb_profit_cost
from monitor_data_exception import cal_balance
from time_job_excute.timeServiceList import TimeService
import logging


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
    TimeService.add_later_service(gt_income_cost.update_gt_income_cost)
    TimeService.add_later_service(hb_profit_cost.update_hb_car_hotel_profit)
    TimeService.add_later_service(hb_profit_cost.update_car_cost_detail)
    TimeService.add_later_service(hb_profit_cost.update_huoli_car_income_type)
    TimeService.add_later_service(hb_profit_cost.update_profit_hotel_income)
    return TimeService


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
    later_service = execute_later_job()
    for fun in later_service.get_later_service():
        try:
            fun_path = fun(1)
            fun_name = fun.__name__
            fun_doc = fun.__doc__
            check_fun = DBCli().redis_cli.sismember("execute_day_job", fun_name)
            if not check_fun:
                if fun_path.endswith("pyc"):
                    fun_path = fun_path[0: -1]
                utils.storage_execute_job(fun_path, fun_name, fun_doc)
                DBCli().redis_cli.sadd("execute_day_job", fun_name)

        except Exception as e:
            logging.warning(str(fun) + "----" + str(e.message) + "---" + str(e.args))
            continue

    check_day_data()
    exception_table = cal_balance()
    if bool(exception_table):
        utils.sendMail("lipenju24@163.com", exception_table, "与前一天的数据有差异")
