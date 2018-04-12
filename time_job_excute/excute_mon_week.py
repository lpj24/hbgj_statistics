from main_service.hb import hb_flight_details, \
    hb_flight_search, hb_flight_focus, hb_activeusers, hb_focus_platform, hb_channel_ticket, hb_airline
from main_service.huoli import car_consumers, hotel_consumers, huoli_buy_consumers
from main_service.gt import gt_activeusers, gt_consumers
from time_job_excute.timeServiceList import TimeService
from monitor import task
import message
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
    # TimeService.add_week_mon_service(hb_flight_details.update_flight_detail_user_weekly)
    # TimeService.add_week_mon_service(hb_consumers.update_hb_consumers_weekly)
    # TimeService.add_week_mon_service(hb_flight_search.update_flight_search_user_weekly)
    TimeService.add_week_mon_service(hb_channel_ticket.update_unable_ticket)
    TimeService.add_week_mon_service(huoli_buy_consumers.update_huoli_buy_consumers_weekly)

    TimeService.add_week_mon_service(hb_airline.update_hbgj_client_airline_inland_weekly)

    return TimeService


if __name__ == "__main__":
    logging.warning("monday execute week data")
    service = add_execute_job()
    for fun in service.get_week_mon_service():
        try:
            fun()
        except Exception as e:
            logging.error(e.message + "---" + str(e.args) + "--" + str(fun))
            continue

    message.sub('execute_week_job', task.check_week_data)
    message.pub('execute_week_job', service.week_job_table)