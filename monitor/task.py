# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from monitor_sql import week_sql
from dbClient import utils
from main_service.gt import gt_income_cost
from main_service.hb import hb_profit_cost, hb_insure
from monitor_data_exception import cal_balance
from time_job_excute.timeServiceList import TimeService
import logging


def check_day_data():
    query_date = DateUtil.get_date_before_days(1)
    query_date = DateUtil.date2str(query_date, '%Y-%m-%d')
    msg = ""
    insert_msg = []
    insert_sql = """
        insert into error_update_table_daily (s_day, job_table)
        values(%s, %s)
    """
    query_table_sql = """
        select GROUP_CONCAT(A.jobTable) from (select
        case when
            LOCATE(' ', job_table) > 0 then CONCAT_ws(',', SUBSTRING_INDEX(job_table, ' ', 1),SUBSTRING_INDEX(job_table, ' ', -1)) ELSE
            job_table end as jobTable
        from bi_execute_job where job_type !=5) A;
    """
    day_sql = DBCli().targetdb_cli.queryOne(query_table_sql)
    day_table = day_sql[0].split(',')
    for execute_sql in day_table:

        format_sql = 'select count(1) from {} where s_day=%s'.format(execute_sql)
        try:
            data = DBCli().targetdb_cli.queryOne(format_sql, [query_date])
        except Exception:
            continue
        if data[0] < 1:
            # error
            insert_msg.append([query_date, execute_sql[0]])
            msg += execute_sql[0] + "<br/>"
        else:
            pass
    if len(msg) > 0:
        DBCli().targetdb_cli.batchInsert(insert_sql, insert_msg)
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
    TimeService.add_later_service(hb_insure.update_insure_type_daily)
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
            utils.storage_execute_job(fun, fun_path)

        except Exception as e:
            logging.warning(str(fun) + "---" + str(e.message) + "---" + str(e.args))
            continue

    check_day_data()
    exception_table = cal_balance()
    if bool(exception_table):
        utils.sendMail("lipenju24@163.com", '<br/>'.join([t for t in exception_table]), "与前一天的数据有差异")