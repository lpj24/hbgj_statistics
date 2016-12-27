from main_service.hb import hb_flight_details, hb_consumers, hb_flight_search, hb_flight_focus, hb_activeusers
from main_service.huoli import eat_activeusers, car_consumers, hotel_activeusers, hotel_consumers
from main_service.gt import gt_consumers, gt_activeusers
from time_job_excute.timeServiceList import TimeService
import logging

if __name__ == "__main__":
    print "excute month"
    # TimeService.add_month_first_service(eat_activeusers.update_eat_active_user_monthly)
    TimeService.add_month_first_service(car_consumers.update_car_consumers_monthly)

    TimeService.add_month_first_service(gt_consumers.update_gtgj_consumers_monthly)
    TimeService.add_month_first_service(gt_activeusers.update_gtgj_activeusers_monthly)

    TimeService.add_month_first_service(hotel_activeusers.update_hotel_activeusers_monthly)

    TimeService.add_month_first_service(hb_activeusers.update_hbgj_activeusers_monthly)

    TimeService.add_month_first_service(hotel_consumers.update_hotel_consumers_monthly)

    TimeService.add_month_first_service(hb_flight_details.update_flight_detail_user_monthly)
    # TimeService.add_month_first_service(hb_consumers.update_hb_consumers_monthly)
    TimeService.add_month_first_service(hb_flight_search.update_flight_search_user_monthly)
    TimeService.add_month_first_service(hb_flight_focus.update_flight_focus_user_monthly)

    for fun in TimeService.get_month_first_service():
        try:
            fun()
        except Exception as e:
            logging.warning(e.message)
            continue


