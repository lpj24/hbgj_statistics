from main_service.hb import hb_consumers, hb_flight_focus, hb_flight_search, hb_flight_details
from main_service.huoli import eat_activeusers, car_consumers
from main_service.gt import gt_consumers
from time_job_excute.timeServiceList import TimeService
import logging


if __name__ == "__main__":
    TimeService.add_quarter_first_service(hb_consumers.update_hb_consumers_quarterly)
    TimeService.add_quarter_first_service(hb_flight_focus.update_flight_focus_user_quarterly)
    TimeService.add_quarter_first_service(hb_flight_search.update_flight_search_user_quarterly)
    TimeService.add_quarter_first_service(hb_flight_details.update_flight_detail_user_quarterly)
    # TimeService.add_quarter_first_service(eat_activeusers.update_eat_active_user_quarterly)
    TimeService.add_quarter_first_service(car_consumers.update_car_consumers_quarterly)

    TimeService.add_quarter_first_service(gt_consumers.update_gtgj_consumers_quarterly)

    for fun in TimeService.get_quarter_first_service():
        try:
            fun()
        except Exception as e:
            logging.warning(e.message + "---" + str(e.args) + "--" + str(fun))
            continue


