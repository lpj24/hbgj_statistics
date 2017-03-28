from time_job_excute.timeServiceList import TimeService
import sys
import logging
from localytics import hb_ticket_book, hbdt_event, hb_pay_type
import time


if __name__ == "__main__":
    days = sys.argv[1]
    TimeService.add_localytics_service(hb_ticket_book.update_booke_ticket_event_hourly)
    TimeService.add_localytics_service(hb_ticket_book.hb_ticket_book)
    TimeService.add_localytics_service(hbdt_event.hbdt_event)
    TimeService.add_localytics_service(hb_pay_type.hb_pay_type)
    TimeService.add_localytics_service(hb_ticket_book.update_ios_android_newuser_daily)

    for fun in TimeService.get_localytics_service():
        try:
            fun(int(days))
            time.sleep(1 * 60 * 61)
        except Exception as e:
            logging.warning(e.message + "---" + str(e.args) + "--" + str(fun))
            continue