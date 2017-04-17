# -*- coding: utf-8 *-
from main_service.hb import hb_activeusers, hb_profit_cost, hb_focus_platform, hb_delay_insure, hb_order, hb_partner, hb_coupon_ticket, \
    hb_focus_newuser, hb_insure, hb_channel_ticket
from main_service.huoli import car_orders, car_consumers, hotel_newusers, hotel_activeusers, \
    hotel_newconsumers, hotel_order, hotel_consumers
from main_service.gt import gt_activeusers, gt_consumers, gt_order, gt_amount, gt_newconsumers, gt_fromHb, \
    gt_income_cost
from main_service.tmp_task import hbgj_users
from time_job_excute.timeServiceList import TimeService
import sys
import logging


def add_execute_job():
    TimeService.add_day_service(car_orders.update_car_orders_daily)
    TimeService.add_day_service(car_consumers.update_car_consumers_daily)
    TimeService.add_day_service(car_consumers.update_car_newconsumers_daily)

    TimeService.add_day_service(gt_activeusers.update_gtgj_newusers_daily)
    TimeService.add_day_service(gt_activeusers.update_gtgj_activeusers_daily)
    TimeService.add_day_service(gt_consumers.update_gtgj_consumers_daily)
    TimeService.add_day_service(gt_order.update_gt_order_daily)

    TimeService.add_day_service(gt_newconsumers.gt_newconsumers_daily)
    TimeService.add_day_service(hotel_activeusers.update_hotel_activeusers_weekly)
    TimeService.add_day_service(hb_activeusers.update_hbgj_activeusers_daily)

    TimeService.add_day_service(hotel_newconsumers.update_hotel_newconsumers_daily)
    TimeService.add_day_service(hotel_order.update_hotel_orders_daily)

    TimeService.add_day_service(hotel_consumers.update_hotel_consumers_daily)
    TimeService.add_day_service(hbgj_users.hbgj_user)

    TimeService.add_day_service(gt_amount.update_gtgj_amount_daily)

    TimeService.add_day_service(gt_fromHb.update_gtgj_from_hb)
    TimeService.add_day_service(gt_income_cost.update_gt_income_cost)

    TimeService.add_day_service(hb_focus_platform.update_focus_platform)
    TimeService.add_day_service(hb_delay_insure.update_hb_deplay_insure)
    TimeService.add_day_service(hb_delay_insure.update_compensate_detail)
    TimeService.add_day_service(hb_order.update_hb_gt_order_daily)
    TimeService.add_day_service(hb_profit_cost.update_huoli_car_income_daily)
    TimeService.add_day_service(hb_profit_cost.update_profit_hb_income)
    TimeService.add_day_service(hb_profit_cost.update_operation_hbgj_channel_ticket_profit_daily)

    TimeService.add_day_service(hb_coupon_ticket.update_hbgj_coupon_tickt)
    TimeService.add_day_service(hb_coupon_ticket.update_gt_coupon_daily)
    TimeService.add_day_service(hb_coupon_ticket.update_huoli_car_coupon_daily)
    TimeService.add_day_service(hb_coupon_ticket.update_huoli_hotel_coupon_daily)
    TimeService.add_day_service(hb_coupon_ticket.update_common_coupon_daily)
    TimeService.add_day_service(hb_coupon_ticket.update_hb_coupon_use_detail_daily)
    TimeService.add_day_service(hb_coupon_ticket.update_coupon_use_detail_daily)
    TimeService.add_day_service(hb_coupon_ticket.update_car_use_detail_daily)
    TimeService.add_day_service(hb_coupon_ticket.update_hotel_use_detail_daily)
    TimeService.add_day_service(hb_focus_newuser.update_fouces_dat_daily)
    TimeService.add_day_service(hb_coupon_ticket.update_gtgj_use_issue_detail_daily)
    TimeService.add_day_service(hb_focus_newuser.update_focus_newuser)

    TimeService.add_day_service(hb_insure.update_hb_insure_daily)
    TimeService.add_day_service(hb_insure.update_insure_type_daily)
    TimeService.add_day_service(hb_insure.update_insure_class_daily)
    # TimeService.add_day_service(hb_focus_newuser.collect_inland_inter_flyid_daily)
    # TimeService.add_day_service(hb_focus_newuser.update_focus_inland_inter_daily)
    TimeService.add_day_service(hb_channel_ticket.update_refund_ticket_channel_daily)
    TimeService.add_day_service(hb_channel_ticket.update_hb_channel_ticket_daily)
    TimeService.add_day_service(hb_channel_ticket.update_hb_channel_ticket_income_daily)
    TimeService.add_day_service(hb_channel_ticket.update_product_ticket_daily)
    TimeService.add_day_service(hb_order.update_operation_hbgj_order_detail_daily)
    TimeService.add_day_service(hb_insure.update_hb_boat)
    TimeService.add_day_service(hotel_newusers.update_hotel_newusers_daily)
    TimeService.add_day_service(hotel_activeusers.update_hotel_activeusers_daily)
    TimeService.add_day_service(hb_partner.update_hb_partner_daily)
    return TimeService


def execute_day_job_again(fun_path, fun_name):
    import os
    fun_path = (fun_path.split("\\"))[-3:]
    fun_path[-1] = (fun_path[-1].split("."))[0]
    fun_path = ".".join(fun_path)
    with open("tmp_py.py", "w") as py_file:
        coding_str = "# -*- coding: utf-8 -*-\n"
        import_str = "from " + fun_path + " import " + fun_name + "\n"
        main_str = "if __name__ == '__main__':\n"
        execute_fun_str = "\t" + fun_name + "(2)" + "\n"
        py_str = coding_str + import_str + main_str + execute_fun_str
        py_file.write(py_str)
    a = os.system("python ./tmp_py.py")
    if a == 0:
        print "update success"

from dbClient.db_client import DBCli
if __name__ == "__main__":
    # days = sys.argv[1]
    import os
    days = 1
    service = add_execute_job()
    bi_execute_day_job = []
    for fun in service.get_day_service():
        try:
            fun_path = fun(int(days))
            if fun_path.endswith("pyc"):
                fun_path = fun_path[0: -1]
            fun_name = fun.__name__
            fun_doc = fun.__doc__

            renewable = 1 if fun_path else 0
            job_type = 1
            if fun_name == "hbgj_user":
                job_type = 5
            bi_execute_day_job.extend([fun_name, fun_path, fun_doc, job_type, renewable])

        except Exception as e:
            logging.warning(str(fun) + "----" + str(e.message) + "---" + str(e.args))
            continue

    insert_sql = """
        insert into bi_execute_job (job_name, job_path, job_doc, job_type, renewable, createtime, updatetime)
        values (%s, %s, %s, %s, %s, now(), now())
    """
    DBCli().targetdb_cli.batchInsert(insert_sql, bi_execute_day_job)