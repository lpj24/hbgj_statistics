import sys, logging
from time_job_excute.timeServiceList import TimeService
from main_service.hb import hb_flight_search, hb_flight_details, hb_flight_focus, hb_first_consumers
from main_service.hb import hb_consumers, hb_ticket_issue_refund
from main_service.tmp_task.hb_search_focus import hb_search_focus
from main_service.huoli import hotel_consumers
import time


if __name__ == "__main__":
    days = sys.argv[1]

    TimeService.add_hard_service(hb_consumers.update_hb_consumers_daily)
    TimeService.add_hard_service(hb_consumers.update_hb_newconsumers_daily)
    TimeService.add_hard_service(hb_consumers.update_hb_consumers_weekly)
    TimeService.add_hard_service(hb_consumers.update_hb_consumers_monthly)

    TimeService.add_hard_service(hb_flight_focus.update_flight_focus_user_daily)

    TimeService.add_hard_service(hb_flight_details.update_dt_detail_uid)
    TimeService.add_hard_service(hb_flight_search.update_dt_search_uid)
    TimeService.add_hard_service(hb_search_focus.write_day)
    TimeService.add_hard_service(hb_flight_focus.update_hb_focus_inter_inland)
    TimeService.add_hard_service(hb_first_consumers.update_hbgj_newconsumers_type_daily)
    TimeService.add_hard_service(hb_first_consumers.update_new_register_user_daily)
    TimeService.add_hard_service(hb_first_consumers.update_hbgj_inter_inland_consumers_daily)

    TimeService.add_hard_service(hb_ticket_issue_refund.update_hbgj_income_issue_refund_daily)
    TimeService.add_hard_service(hb_ticket_issue_refund.update_hbgj_cost_type_daily)
    TimeService.add_hard_service(hb_flight_search.update_flight_search_user_daily)
    TimeService.add_hard_service(hb_flight_details.update_flight_detail_user_daily)

    for fun in TimeService.get_hard_service():
        try:
            fun(int(days))
        except AssertionError as e:
            TimeService.add_hard_service(fun)
            time.sleep(1 * 60 * 10)
            continue
        except Exception as e:
            logging.warning(str(e.message) + "---" + str(e.args) + "--" + str(fun))
            continue