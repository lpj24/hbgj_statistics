from main_service.hb import hb_flight_details, hb_consumers, \
    hb_flight_search, hb_flight_focus, hb_activeusers, hb_focus_platform, hb_channel_ticket
from main_service.huoli import car_consumers, hotel_consumers
from main_service.gt import gt_activeusers, gt_consumers
from time_job_excute.timeServiceList import TimeService
from monitor import task
import logging


def add_execute_job():
    TimeService.add_week_mon_service(car_consumers.update_car_consumers_weekly)

    TimeService.add_week_mon_service(gt_activeusers.update_gtgj_activeusers_weekly)
    TimeService.add_week_mon_service(gt_consumers.update_gtgj_consumers_weekly)

    TimeService.add_week_mon_service(hb_activeusers.update_hbgj_activeusers_weekly)

    TimeService.add_week_mon_service(hotel_consumers.update_hotel_consumers_weekly)
    TimeService.add_week_mon_service(hb_channel_ticket.update_hb_company_ticket_weekly)
    TimeService.add_week_mon_service(hb_channel_ticket.update_supplier_refused_order_weekly)

    TimeService.add_week_mon_service(hb_focus_platform.update_focus_platform_weekly)
    TimeService.add_week_mon_service(hb_flight_focus.update_flight_focus_user_weekly)
    TimeService.add_week_mon_service(hb_flight_details.update_flight_detail_user_weekly)
    # TimeService.add_week_mon_service(hb_consumers.update_hb_consumers_weekly)
    TimeService.add_week_mon_service(hb_flight_search.update_flight_search_user_weekly)
    TimeService.add_week_mon_service(hb_channel_ticket.update_unable_ticket)

    TimeService.add_week_mon_service(task.check_week_data)
    return TimeService

if __name__ == "__main__":
    print "monday excute week"
    service = add_execute_job()
    # TimeService.add_week_mon_service(eat_activeusers.update_eat_active_user_weekly)
    for fun in service.get_week_mon_service():
        try:
            fun()
        except Exception as e:
            logging.warning(e.message + "---" + str(e.args) + "--" + str(fun))
            continue

