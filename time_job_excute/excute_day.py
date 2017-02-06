from main_service.hb import hb_activeusers, hb_profit_cost, hb_focus_platform, hb_delay_insure
from main_service.huoli import car_orders, car_consumers, hotel_newusers, hotel_activeusers, \
    hotel_newconsumers, hotel_order, hotel_consumers
from main_service.gt import gt_activeusers, gt_consumers, gt_order, gt_amount, gt_newconsumers, gt_fromHb, \
    gt_income_cost
from main_service.tmp_task import hbgj_users
from time_job_excute.timeServiceList import TimeService
import sys
import logging


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

    TimeService.add_day_service(hotel_consumers.update_hotel_consumers_daily)

    TimeService.add_day_service(hbgj_users.hbgj_user)
    TimeService.add_day_service(gt_fromHb.update_gtgj_from_hb)
    TimeService.add_day_service(gt_income_cost.update_gt_income_cost)
    TimeService.add_day_service(hb_profit_cost.update_hb_car_hotel_profit)
    TimeService.add_day_service(hb_focus_platform.update_focus_platform)
    TimeService.add_day_service(hb_delay_insure.update_hb_deplay_insure)
    TimeService.add_day_service(hb_delay_insure.update_compensate_detail)

    for fun in TimeService.get_day_service():
        try:
            fun(int(days))
        except Exception as e:
            logging.warning(e.message + "---" + str(e.args) + "--" + str(fun))
            continue
