import logging
import sys
import time

from main_service.localytics import hb_ticket_book, hb_pay_type

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

    for fun in TimeService.get_localytics_service():
        try:
            fun_path = fun(int(days))
            fun_name = fun.__name__
            fun_doc = fun.__doc__
            check_fun = DBCli().redis_cli.sismember("execute_day_job", fun_name)
            if not check_fun:
                if fun_path and fun_path.endswith("pyc"):
                    fun_path = fun_path[0: -1]
                utils.storage_execute_job(fun_path, fun_name, fun_doc)
                DBCli().redis_cli.sadd("execute_day_job", fun_name)

            time.sleep(1 * 60 * 61)
        except (Exception, AssertionError) as e:
            TimeService.add_localytics_service(fun)
            logging.warning(e.message + "---" + str(e.args) + "--" + str(fun))
            continue