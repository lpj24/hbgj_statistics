# coding: utf-8
import sys
from main_service.gt import gt_activeusers, gt_consumers, gt_order, gt_amount, gt_newconsumers, gt_fromHb, gt_register_user
from main_service.hb import hb_focus_platform, hb_delay_insure, hb_order, hb_partner, \
    hb_coupon_ticket, hb_focus_newuser, hb_insure, hbgj_users, hb_channel_client, hb_coupon_email
from main_service.huoli import car_orders, car_consumers, hotel_activeusers, \
    hotel_newconsumers, hotel_order, hotel_consumers, hotel_newusers, huoli_buy_consumers
from time_job_excute.timeServiceList import TimeService
import Queue
from dbClient.utils import execute_job_thread_pool


def add_execute_job():
    TimeService.add_day_service(car_orders.update_car_orders_daily)
    TimeService.add_day_service(car_consumers.update_car_consumers_daily)
    TimeService.add_day_service(car_consumers.update_car_newconsumers_daily)

    TimeService.add_day_service(gt_activeusers.update_gtgj_newusers_daily)
    TimeService.add_day_service(gt_activeusers.update_gtgj_activeusers_daily)
    TimeService.add_day_service(gt_consumers.update_gtgj_consumers_daily)
    TimeService.add_day_service(gt_order.update_gt_order_daily)
    TimeService.add_day_service(gt_order.update_hb_gt_book_daily)
    TimeService.add_day_service(gt_newconsumers.gt_newconsumers_daily)
    TimeService.add_day_service(hotel_activeusers.update_hotel_activeusers_weekly)

    TimeService.add_day_service(hotel_newconsumers.update_hotel_newconsumers_daily)
    TimeService.add_day_service(hotel_order.update_hotel_orders_daily)
    TimeService.add_day_service(hotel_consumers.update_hotel_consumers_daily)
    TimeService.add_day_service(hbgj_users.hbgj_user)

    TimeService.add_day_service(gt_fromHb.update_gtgj_from_hb)

    TimeService.add_day_service(hb_focus_platform.update_focus_platform)
    TimeService.add_day_service(hb_delay_insure.update_hb_deplay_insure)
    TimeService.add_day_service(hb_delay_insure.update_compensate_detail)
    TimeService.add_day_service(hb_order.update_hb_gt_order_daily)
    TimeService.add_day_service(hb_order.update_hbgj_ticket_region_inter_daily)

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
    TimeService.add_day_service(hb_coupon_ticket.update_coupon_list)
    TimeService.add_day_service(hb_insure.update_hb_insure_daily)

    TimeService.add_day_service(hb_insure.update_insure_class_daily)
    TimeService.add_day_service(hb_order.update_operation_hbgj_order_detail_daily)
    TimeService.add_day_service(hb_insure.update_hb_boat)

    TimeService.add_day_service(hotel_newusers.update_hotel_newusers_daily)
    TimeService.add_day_service(hb_partner.update_hb_partner_daily)
    TimeService.add_day_service(gt_amount.update_gtgj_amount_daily)
    TimeService.add_day_service(hb_order.update_hb_gt_order_new_daily)
    TimeService.add_day_service(gt_register_user.update_gtgj_register_user_daily)
    TimeService.add_day_service(gt_register_user.update_hbgj_register_user_daily)

    TimeService.add_day_service(huoli_buy_consumers.update_huoli_buy_orders_daily)
    TimeService.add_day_service(huoli_buy_consumers.update_huoli_buy_consumers_daily)
    TimeService.add_day_service(huoli_buy_consumers.update_huoli_buy_newconsumers_daily)
    TimeService.add_day_service(huoli_buy_consumers.update_huoli_edj_newconsumers_daily)
    TimeService.add_day_service(hb_order.update_hbgj_h5_ticket_daily)

    TimeService.add_day_service(hb_channel_client.update_hbgj_channel_client_ticket_daily)
    TimeService.add_day_service(hb_channel_client.update_hbgj_channel_client_ticket_h5_daily)
    TimeService.add_day_service(hb_insure.update_insure_type_detail_daily)
    TimeService.add_day_service(hb_coupon_email.send_hb_coupon_delay_eamil_daily)
    return TimeService


if __name__ == "__main__":
    days = sys.argv[1]
    service = add_execute_job()
    day_q = Queue.Queue()
    for job in service.get_day_service():
        day_q.put(job)

    execute_job_thread_pool(day_q, days)
    day_q.join()  # 等待队里为空之后再执行以下操作
