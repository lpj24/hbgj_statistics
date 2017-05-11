sql = [
    "select count(1) from huoli_car_orders_daily where s_day=%s",
    "select count(1) from huoli_car_consumers_daily where s_day=%s",
    "select count(1) from huoli_car_newconsumers_daily where s_day=%s",
    "select count(1) from gtgj_newusers_daily where s_day=%s",
    "select count(1) from gtgj_activeusers_daily where s_day=%s",
    "select count(1) from gtgj_consumers_daily where s_day=%s",
    "select count(1) from gtgj_amount_daily where s_day=%s",
    "select count(1) from gtgj_newconsumers_daily where s_day=%s",
    "select count(1) from hotel_newusers_daily where s_day=%s",
    "select count(1) from hotel_activeusers_daily where s_day=%s",
    "select count(1) from hbgj_activeusers_daily where s_day=%s",
    "select count(1) from hotel_orders_daily where s_day=%s",


    "select count(1) from hbgj_consumers_daily where s_day=%s",
    "select count(1) from hbgj_newconsumers_daily where s_day=%s",
    "select count(1) from hotel_consumers_daily where s_day=%s",
    "select count(1) from hbgj_newconsumers_daily where s_day=%s",

    "select count(1) from hbdt_details_daily where s_day=%s",
    "select count(1) from hbdt_search_daily where s_day=%s",
    "select count(1) from hbdt_focus_daily where s_day=%s",
    "select count(1) from hbgj_event_orderpay_paytype_ios_android_daily where s_day=%s",
    "select count(1) from ticket_book_event where s_day=%s",
    "select count(1) from hbdt_event where s_day=%s",
    "select count(1) from gtgj_ticket_from_hb where s_day=%s",
    "select count(1) from profit_gt_cost where s_day=%s",
    "select count(1) from profit_gt_income where s_day=%s",

    "select count(1) from profit_hb_cost where s_day=%s",
    "select count(1) from profit_huoli_car_cost where s_day=%s",
    "select count(1) from profit_huoli_hotel_cost where s_day=%s",
    "select count(1) from operation_hbgj_delay_treasure_daily where s_day=%s",
    "select count(1) from operation_delaycare_detail_daily where s_day=%s",
    "select count(1) from hbgj_order_detail_daily where s_day=%s",
    "select count(1) from profit_huoli_car_income where s_day=%s",
    "select count(1) from ticket_book_event_hourly where s_day=%s",
    "select count(1) from hbdt_flight_partnerapi where s_day=%s",

    "select count(1) from coupon_huoli_car where s_day=%s",
    "select count(1) from coupon_hbgj_ticket where s_day=%s",
    "select count(1) from coupon_huoli_hotel where s_day=%s",
    "select count(1) from coupon_common where s_day=%s",
    "select count(1) from coupon_gtgj_ticket where s_day=%s",

    "select count(1) from hbdt_focus_newusers_daily where s_day=%s",
    "select count(1) from coupon_hbgj_ticket_use_detail_client where s_day=%s",
    "select count(1) from coupon_hbgj_ticket_use_detail where s_day=%s",

    "select count(1) from coupon_gtgj_ticket_use_detail where s_day=%s",
    "select count(1) from coupon_huoli_car_use_detail where s_day=%s",
    "select count(1) from coupon_huoli_hotel_use_detail where s_day=%s",
    "select count(1) from coupon_gtgj_ticket_issue_detail where s_day=%s",
    "select count(1) from coupon_issue_detail where s_day=%s",

    "select count(1) from operation_hbgj_insure_type_daily where s_day=%s",
    "select count(1) from operation_hbgj_insure_platform_daily where s_day=%s",
    "select count(1) from operation_hbgj_insure where s_day=%s",
    "select count(1) from profit_hb_income where s_day=%s",

    "select count(1) from profit_huoli_car_income_type where s_day=%s",
    "select count(1) from profit_huoli_car_cost_type where s_day=%s",
    "select count(1) from profit_huoli_hotel_income where s_day=%s",
    "select count(1) from operation_hbgj_channel_refund_ticket_daily where s_day=%s",
    "select count(1) from operation_hbgj_channel_ticket_daily where s_day=%s",

    "select count(1) from operation_accident_insure_refund_detail_daily where s_day=%s",
    "select count(1) from operation_accident_insure_detail_daily where s_day=%s",
    "select count(1) from operation_hbgj_order_detail_daily where s_day=%s",
    "select count(1) from operation_hbgj_channel_ticket_income_daily where s_day=%s",
    "select count(1) from ticket_query_newuser_daily where s_day=%s",
    "select count(1) from operation_hbgj_channel_ticket_profit_daily where s_day=%s",
    "select count(1) from operation_hbgj_supplier_ticket_daily where s_day=%s",
    "select count(1) from hbdt_focus_daily_inter where s_day=%s",
    "select count(1) from hbgj_newconsumers_type_daily where s_day=%s",
    "select count(1) from hbgj_new_register_user_daily where s_day=%s",
    "select count(1) from hbgj_inter_inland_consumers_daily where s_day=%s",
    "select count(1) from profit_hb_cost_type_daily where s_day=%s",
    "select count(1) from profit_hb_income_type_daily where s_day=%s",
    "select count(1) from profit_hb_self_no_transfer_daily where s_day=%s",
    "select count(1) from profit_hb_self_transfer_daily where s_day=%s",
    "select count(1) from profit_hb_supply_transfer_daily where s_day=%s",
    "select count(1) from profit_hb_supply_no_transfer_daily where s_day=%s",
    "select count(1) from operation_hbgj_amount_monitor_cz where s_day=%s",
    "select count(1) from operation_hbgj_amount_monitor_hlth where s_day=%s",
    "select count(1) from operation_hbgj_amount_monitor_hlth_SZX where s_day=%s",
    "select count(1) from operation_hbgj_amount_monitor_inter where s_day=%s",
    "select count(1) from operation_hbgj_qp_success where s_day=%s",
    "select count(1) from profit_huoli_fmall_cost where s_day=%s",
    "select count(1) from profit_huoli_buy_cost where s_day=%s",
    "select count(1) from hbgj_newusers_daily where s_day=%s",
]

week_sql = [
    "select count(1) from huoli_car_consumers_weekly where s_day = %s",
    "select count(1) from gtgj_activeusers_weekly where s_day = %s",
    "select count(1) from gtgj_consumers_weekly where s_day = %s",
    "select count(1) from hbgj_activeusers_weekly where s_day = %s",
    "select count(1) from hotel_consumers_weekly where s_day = %s",
    "select count(1) from operation_hbgj_channel_ticket_daily where s_day = %s",
    "select count(1) from operation_hbgj_company_ticket_weekly where s_day = %s",
    "select count(1) from hbdt_focus_platform_weekly where s_day = %s",
    "select count(1) from hbdt_focus_weekly where s_day = %s",
    "select count(1) from hbdt_details_weekly where s_day = %s",
    "select count(1) from hbdt_search_weekly where s_day = %s",
    "select count(1) from operation_hbgj_unable_ticket_weekly where s_day = %s",
    "select count(1) from operation_hbgj_human_intervention_ticket_weekly where s_day = %s",
]