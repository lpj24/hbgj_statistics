import logging
import sys
import time

from main_service.localytics import hb_ticket_book, hb_pay_type, hb_stages

from dbClient import utils
from dbClient.db_client import DBCli
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

    for fun in TimeService.get_localytics_service():
        try:
            fun_path = fun(int(days))
            utils.storage_execute_job(fun, fun_path)
            time.sleep(1 * 60 * 20)
        except (Exception, AssertionError) as e:
            TimeService.add_localytics_service(fun)
            logging.warning(e.message + "---" + str(e.args) + "--" + str(fun))
            continue

