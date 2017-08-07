import sys
from time_job_excute.timeServiceList import TimeService
from main_service.hb import hb_flight_search, hb_flight_details, hb_flight_focus, hb_first_consumers
from main_service.hb import hb_consumers, hb_ticket_issue_refund, hb_company_amount, \
    hb_channel_ticket, hb_profit_cost, hb_activeusers, hb_coupon_ticket, hb_focus_newuser
from main_service.huoli import hotel_activeusers
from main_service.gt import gt_income_cost
from main_service.tmp_task.hb_search_focus import hb_search_focus
import Queue
from dbClient.utils import execute_job_thread_pool


def add_execute_job():
    TimeService.add_hard_service(hb_focus_newuser.update_focus_newuser)
    TimeService.add_hard_service(hb_consumers.update_hb_consumers_daily)
    TimeService.add_hard_service(hb_consumers.update_hb_newconsumers_daily)
    TimeService.add_hard_service(hb_consumers.update_hb_consumers_weekly)
    TimeService.add_hard_service(hb_consumers.update_hb_consumers_monthly)
    TimeService.add_hard_service(hb_consumers.update_hbgj_newconsumers_inter_daily)
    TimeService.add_hard_service(hb_consumers.update_hbgj_consumers_inter_daily)
    TimeService.add_hard_service(hb_consumers.update_hbgj_newconsumers_inter_daily_nation)

    TimeService.add_hard_service(hb_flight_focus.update_flight_focus_user_daily)

    TimeService.add_hard_service(hb_flight_focus.update_hb_focus_inter_inland)
    TimeService.add_hard_service(hb_first_consumers.update_hbgj_newconsumers_type_daily)
    TimeService.add_hard_service(hb_first_consumers.update_new_register_user_daily)
    TimeService.add_hard_service(hb_first_consumers.update_hbgj_inter_inland_consumers_daily)

    TimeService.add_hard_service(hb_ticket_issue_refund.update_hbgj_income_issue_refund_daily)
    TimeService.add_hard_service(hb_ticket_issue_refund.update_hbgj_cost_type_daily)

    TimeService.add_hard_service(hb_ticket_issue_refund.update_profit_hb_self_no_transfer_daily)
    TimeService.add_hard_service(hb_ticket_issue_refund.update_profit_hb_self_transfer_daily)
    TimeService.add_hard_service(hb_ticket_issue_refund.update_profit_hb_supply_no_transfer_daily)
    TimeService.add_hard_service(hb_ticket_issue_refund.update_profit_hb_supply_transfer_daily)

    TimeService.add_hard_service(hb_company_amount.update_operation_hbgj_amount_monitor_hlth_szx)
    TimeService.add_hard_service(hb_company_amount.update_operation_hbgj_amount_monitor_hlth)
    TimeService.add_hard_service(hb_company_amount.update_operation_hbgj_qp_success)
    TimeService.add_hard_service(hb_company_amount.update_operation_hbgj_amount_monitor_cz)
    TimeService.add_hard_service(hb_company_amount.update_operation_hbgj_amount_monitor_inter)
    TimeService.add_hard_service(hb_company_amount.update_operation_hbgj_special_return_daily)
    TimeService.add_hard_service(hb_channel_ticket.update_refund_ticket_channel_daily)
    TimeService.add_hard_service(hb_channel_ticket.update_hb_channel_ticket_daily)
    TimeService.add_hard_service(hb_channel_ticket.update_hb_channel_ticket_income_daily)
    TimeService.add_hard_service(hb_channel_ticket.update_product_ticket_daily)

    TimeService.add_hard_service(hb_profit_cost.update_huoli_car_income_daily)
    TimeService.add_hard_service(hb_profit_cost.update_profit_hb_income)
    TimeService.add_hard_service(hb_profit_cost.update_operation_hbgj_channel_ticket_profit_daily)
    TimeService.add_hard_service(gt_income_cost.update_gt_income_cost)

    TimeService.add_hard_service(hb_activeusers.update_hbgj_activeusers_daily)
    TimeService.add_hard_service(hb_activeusers.update_hbgj_newuser_daily)
    TimeService.add_hard_service(hb_channel_ticket.update_operation_hbgj_obsolete_order_daily)
    TimeService.add_hard_service(hotel_activeusers.update_hotel_activeusers_daily)

    TimeService.add_hard_service(hb_flight_search.update_flight_search_user_daily)
    TimeService.add_hard_service(hb_flight_details.update_flight_detail_user_daily)
    TimeService.add_hard_service(hb_flight_details.update_dt_detail_uid)
    TimeService.add_hard_service(hb_flight_search.update_dt_search_uid)
    TimeService.add_hard_service(hb_search_focus.write_day)
    TimeService.add_hard_service(hb_coupon_ticket.update_profit_huoli_fmall_cost)
    TimeService.add_hard_service(hb_coupon_ticket.update_profit_huoli_buy_cost)
    return TimeService

if __name__ == "__main__":
    days = sys.argv[1]
    service = add_execute_job()
    hard_tmp_q = Queue.Queue()
    for job in service.get_hard_service():
        hard_tmp_q.put(job)

    execute_job_thread_pool(hard_tmp_q, days)
    hard_tmp_q.join()
    print "hard job finished"
