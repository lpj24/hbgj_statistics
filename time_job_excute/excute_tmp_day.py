import sys, logging
from time_job_excute.timeServiceList import TimeService
from main_service.hb import hb_flight_search, hb_flight_details, hb_flight_focus
from main_service.hb import hb_consumers, hb_focus_newuser
from main_service.tmp_task.hb_search_focus import hb_search_focus
from main_service.huoli import hotel_consumers


if __name__ == "__main__":
    days = sys.argv[1]

    TimeService.add_hard_service(hb_consumers.update_hb_consumers_daily)
    TimeService.add_hard_service(hb_consumers.update_hb_newconsumers_daily)
    TimeService.add_hard_service(hb_consumers.update_hb_consumers_weekly)
    TimeService.add_hard_service(hb_consumers.update_hb_consumers_monthly)

    TimeService.add_hard_service(hb_flight_focus.update_flight_focus_user_daily)

    TimeService.add_hard_service(hb_flight_details.update_dt_detail_uid)
    TimeService.add_hard_service(hb_flight_search.update_dt_search_uid)
    TimeService.add_hard_service(hb_flight_details.update_flight_detail_user_daily)
    TimeService.add_hard_service(hb_flight_search.update_flight_search_user_daily)
    TimeService.add_hard_service(hb_search_focus.write_day)

    #tmp task
    # TimeService.add_day_service(hb_focus_newuser.collect_his_phone_uid)

    for fun in TimeService.get_hard_service():
        try:
            fun(int(days))
        except Exception as e:
            logging.warning(e.message + str(fun))
            continue