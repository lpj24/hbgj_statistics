# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from dbClient import utils
from main_service.gt import gt_income_cost
from main_service.hb import hb_profit_cost, hb_insure
from monitor_data_exception import cal_balance
from time_job_excute.timeServiceList import TimeService
import logging
from itertools import chain


def check_day_data():
    query_date = DateUtil.get_date_before_days(1)
    query_date = DateUtil.date2str(query_date, '%Y-%m-%d')
    msg = ""
    insert_msg = set()
    insert_sql = """
        insert into error_update_table_daily (s_day, error_job_id)
        values(%s, %s)
    """

    query_table_sql = """
        select id, job_table
        from bi_execute_job where job_type !=5;
    """
    day_table = DBCli().targetdb_cli.query_all(query_table_sql)
    query_table = [table for table in day_table]

    for table in query_table:
        j_id, job_table = table
        for t in job_table.split(' '):
            format_sql = 'select count(1) from {} where s_day=%s'.format(t)
            if t.find('weekly') >= 0 or t.find('monthly') >= 0:
                continue
            try:
                data = DBCli().targetdb_cli.query_one(format_sql, [query_date])
            except Exception as e:
                logging.error(e)
                continue
            if data[0] < 1:
                # error
                insert_msg.add(j_id)
                msg += t + "<br/>"
    if len(msg) > 0:
        insert_msg = [[query_date, ids] for ids in list(insert_msg)]
        DBCli().targetdb_cli.batch_insert(insert_sql, insert_msg)
        utils.sendMail("762575190@qq.com", msg, u"数据查询异常")
    else:
        utils.sendMail("762575190@qq.com", u"数据查询正常", u"数据查询正常")


def check_week_data(table_list):
    start_week, _ = DateUtil.get_last_week_date()
    query_date = DateUtil.date2str(start_week, '%Y-%m-%d')
    msg = ""
    for table in table_list:
        execute_sql = 'select count(1) from {} where s_day = %s'.format(table)
        data = DBCli().targetdb_cli.query_one(execute_sql, [query_date])
        if data[0] < 1:
            # error
            msg += table + "<br/>"

    if len(msg) > 0:
        utils.sendMail("762575190@qq.com", msg, u"周数据查询异常")
    else:
        utils.sendMail("762575190@qq.com", u"周数据查询正常", u"周数据查询正常")


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
        if not job.__doc__:
            raise AttributeError(str(job.__name__) + ' 没有doc描述')
        else:
            logging.warning(job)

    for week_job in week_service.get_week_mon_service():
        if not week_job.__doc__:
            raise AttributeError(str(week_job.__name__) + ' 没有doc描述')
        else:
            logging.warning(week_job)

    for month_job in month_service.get_month_first_service():
        logging.warning(month_job)


if __name__ == "__main__":
    # import subprocess
    # later_service = execute_later_job()
    # for fun in later_service.get_later_service():
    #     try:
    #         fun_path = fun(1)
    #     except Exception as e:
    #         logging.error(str(fun) + "---" + str(e.message) + "---" + str(e.args))
    #         continue
    #     finally:
    #         utils.storage_execute_job(fun)
    check_day_data()
    # exception_table = cal_balance()
    #
    # if exception_table:
    #     utils.sendMail("762575190@qq.com", '<br/>'.join([t for t in exception_table]), "与前一天的数据有差异")
    # subprocess.Popen("ps aux | grep excute_tmp_day*| grep -v grep|awk '{print $2}'|xargs kill -9",
    #                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)