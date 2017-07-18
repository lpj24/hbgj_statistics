from main_service.huoli import eat_activeusers, car_orders, car_consumers, hotel_order
from main_service.gt import gt_order, gt_amount, gt_consumers, gt_neworder, gt_income_cost
from time_job_excute.timeServiceList import TimeService
import logging


if __name__ == "__main__":
    # TimeService.add_hour_service(eat_activeusers.update_eat_active_user_daily)
    TimeService.add_hour_service(car_orders.update_car_orders_daily)
    TimeService.add_hour_service(car_consumers.update_car_consumers_daily)
    TimeService.add_hour_service(car_consumers.update_car_newconsumers_daily)

    TimeService.add_hour_service(gt_order.update_gt_order_daily)
    TimeService.add_hour_service(gt_order.update_gt_order_hourly)
    TimeService.add_hour_service(gt_consumers.update_gtgj_consumers_daily)

    TimeService.add_hour_service(gt_neworder.update_gt_order_daily)
    TimeService.add_hour_service(hotel_order.update_hotel_orders_daily)
    TimeService.add_hour_service(gt_income_cost.update_gt_income_cost)
    # TimeService.add_hour_service(gt_amount.update_gtgj_amount_daily)

    for fun in TimeService.get_hour_service():
        try:
            fun()
        except Exception as e:
            logging.warning(e.message + "---" + str(e.args) + "--" + str(fun))
            continue



