# -*- coding: utf-8 *-
from main_service.hb import hb_activeusers, hb_focus_platform, hb_delay_insure, hb_order, hb_partner, \
    hb_coupon_ticket, hb_focus_newuser, hb_insure
from main_service.huoli import car_orders, car_consumers, hotel_newusers, hotel_activeusers, \
    hotel_newconsumers, hotel_order, hotel_consumers
from main_service.gt import gt_activeusers, gt_consumers, gt_order, gt_amount, gt_newconsumers, gt_fromHb, \
    gt_income_cost
from main_service.tmp_task import hbgj_users
from dbClient.db_client import DBCli
from dbClient import utils
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

    TimeService.add_day_service(gt_fromHb.update_gtgj_from_hb)

    TimeService.add_day_service(hb_focus_platform.update_focus_platform)
    TimeService.add_day_service(hb_delay_insure.update_hb_deplay_insure)
    TimeService.add_day_service(hb_delay_insure.update_compensate_detail)
    TimeService.add_day_service(hb_order.update_hb_gt_order_daily)

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
    TimeService.add_day_service(hb_order.update_operation_hbgj_order_detail_daily)
    TimeService.add_day_service(hb_insure.update_hb_boat)
    TimeService.add_day_service(hotel_newusers.update_hotel_newusers_daily)
    TimeService.add_day_service(hotel_activeusers.update_hotel_activeusers_daily)
    TimeService.add_day_service(hb_partner.update_hb_partner_daily)
    TimeService.add_day_service(gt_amount.update_gtgj_amount_daily)
    return TimeService


if __name__ == "__main__":
    days = sys.argv[1]
    service = add_execute_job()
    for fun in service.get_day_service():
        try:
            fun_path = fun(int(days))
            fun_name = fun.__name__
            fun_doc = fun.__doc__
            check_fun = DBCli().redis_cli.sismember("execute_day_job", fun_name)
            if not check_fun:
                if fun_path and fun_path.endswith("pyc"):
                    fun_path = fun_path[0: -1]
                utils.storage_execute_job(fun_path, fun_name, fun_doc)
                DBCli().redis_cli.sadd("execute_day_job", fun_name)

        except Exception as e:
            logging.warning(str(fun) + "----" + str(e.message) + "---" + str(e.args))
            continue
