import logging
import sys
import time
from main_service.localytics import hb_ticket_book, hb_pay_type, hb_stages, hb_nameauth, hb_phoneverify, gt_travel
from dbClient import utils
from main_service.localytics import hbdt_event
from time_job_excute.timeServiceList import TimeService


if __name__ == "__main__":
    days = sys.argv[1]
    TimeService.add_localytics_service(hb_ticket_book.update_booke_ticket_event_hourly)
    TimeService.add_localytics_service(hb_ticket_book.hb_ticket_book)
    TimeService.add_localytics_service(hbdt_event.hbdt_event)
    TimeService.add_localytics_service(hb_pay_type.hb_pay_type)
    TimeService.add_localytics_service(hb_ticket_book.update_ios_android_newuser_daily)
    TimeService.add_localytics_service(hb_stages.update_hbgj_stages_daily)
    TimeService.add_localytics_service(hb_stages.update_weex_activated_type_daily)

    TimeService.add_localytics_service(hb_nameauth.update_weex_phoneverify)
    TimeService.add_localytics_service(hb_phoneverify.update_weex_phoneverify)
    TimeService.add_localytics_service(gt_travel.hb_gt_travel_daily)
    TimeService.add_localytics_service(gt_travel.station_pv_uv)

    for fun in TimeService.get_localytics_service():
        try:
            fun_path = fun(int(days))
            utils.storage_execute_job(fun)
            time.sleep(1 * 60 * 20)
        except (Exception, AssertionError) as e:
            # TimeService.add_localytics_service(fun)
            logging.error("localytics error" + e.message + "---" + str(e.args) + "--" + str(fun))
            continue

