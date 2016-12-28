from main_service.hb import hb_consumers, hb_activeusers
from main_service.huoli import car_orders, car_consumers, hotel_newusers, hotel_activeusers, \
    hotel_newconsumers, hotel_order, hotel_consumers
from main_service.gt import gt_activeusers, gt_consumers, gt_order, gt_amount, gt_newconsumers, gt_fromHb
from main_service.tmp_task import hbgj_users
from localytics import hbdt_event, hb_ticket_book, hb_pay_type
from time_job_excute.timeServiceList import TimeService
import sys, logging


if __name__ == "__main__":
    days = sys.argv[1]
    # TimeService.add_day_service(eat_activeusers.update_eat_active_user_daily)
    TimeService.add_day_service(car_orders.update_car_orders_daily)
    TimeService.add_day_service(car_consumers.update_car_consumers_daily)
    TimeService.add_day_service(car_consumers.update_car_newconsumers_daily)

    TimeService.add_day_service(gt_activeusers.update_gtgj_newusers_daily)
    TimeService.add_day_service(gt_activeusers.update_gtgj_activeusers_daily)
    TimeService.add_day_service(gt_consumers.update_gtgj_consumers_daily)
    TimeService.add_day_service(gt_order.update_gt_order_daily)
    TimeService.add_day_service(gt_amount.update_gtgj_amount_daily)

    TimeService.add_day_service(gt_newconsumers.gt_newconsumers_daily)
    TimeService.add_day_service(hotel_newusers.update_hotel_newusers_daily)
    TimeService.add_day_service(hotel_activeusers.update_hotel_activeusers_daily)
    TimeService.add_day_service(hotel_activeusers.update_hotel_activeusers_weekly)
    TimeService.add_day_service(hb_activeusers.update_hbgj_activeusers_daily)

    TimeService.add_day_service(hotel_newconsumers.update_hotel_newconsumers_daily)
    TimeService.add_day_service(hotel_order.update_hotel_orders_daily)

    # TimeService.add_day_service(hb_consumers.update_hb_consumers_daily)
    # TimeService.add_day_service(hb_consumers.update_hb_newconsumers_daily)
    # TimeService.add_day_service(hb_consumers.update_hb_consumers_weekly)
    # TimeService.add_day_service(hb_consumers.update_hb_consumers_monthly)
    TimeService.add_day_service(hotel_consumers.update_hotel_consumers_daily)

    TimeService.add_day_service(hbgj_users.hbgj_user)
    TimeService.add_day_service(gt_fromHb.update_gtgj_from_hb)
    # TimeService.add_day_service(hbdt_event.hbdt_event)
    # TimeService.add_day_service(hb_ticket_book.hb_ticket_book)
    #
    # TimeService.add_day_service(gt_neworder.update_gt_order_daily)
    # TimeService.add_day_service(hb_pay_type.hb_pay_type)

    # TimeService.add_day_service(hb_flight_focus.update_flight_focus_user_daily)
    # TimeService.add_day_service(hb_flight_details.update_flight_detail_user_daily)
    # TimeService.add_day_service(hb_flight_search.update_flight_search_user_daily)

    for fun in TimeService.get_day_service():
        try:
            fun(int(days))
        except Exception as e:
            logging.warning(e.message + str(fun))
            continue
