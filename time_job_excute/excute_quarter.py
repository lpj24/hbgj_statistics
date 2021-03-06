from main_service.hb import hb_consumers, hb_flight_focus, hb_flight_search, hb_flight_details
from main_service.huoli import car_consumers, huoli_buy_consumers
from main_service.gt import gt_consumers
from time_job_excute.timeServiceList import TimeService
import logging


if __name__ == "__main__":
    TimeService.add_quarter_first_service(hb_consumers.update_hb_consumers_quarterly)
    TimeService.add_quarter_first_service(hb_consumers.update_hbgj_consumers_inter_quarterly)
    TimeService.add_quarter_first_service(hb_flight_focus.update_flight_focus_user_quarterly)
    TimeService.add_quarter_first_service(hb_flight_search.update_flight_search_user_quarterly)
    TimeService.add_quarter_first_service(hb_flight_details.update_flight_detail_user_quarterly)
    # TimeService.add_quarter_first_service(eat_activeusers.update_eat_active_user_quarterly)
    TimeService.add_quarter_first_service(car_consumers.update_car_consumers_quarterly)

    TimeService.add_quarter_first_service(gt_consumers.update_gtgj_consumers_quarterly)
    TimeService.add_quarter_first_service(gt_consumers.storage_gt_consumers_quarter)

    TimeService.add_quarter_first_service(huoli_buy_consumers.update_huoli_buy_consumers_quarterly)

    for fun in TimeService.get_quarter_first_service():
        try:
            fun()
        except Exception as e:
            logging.error(e.message + "---" + str(e.args) + "--" + str(fun))
            continue


