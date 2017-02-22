from hbsql import ticket_order_num4, hb_orders_date_sql, hb_consumers, hb_flight_details, \
    hb_flight_search, hb_flight_focus, hb_activeusers, hb_order, coupon_ticket
handler_sql = {
    'ticket_order_num_sql': ticket_order_num4.ticket_order_num_sql,
    'hb_ticket_test_sql': ticket_order_num4.hb_ticket_test_sql,
    'target_order_ticket_sql': ticket_order_num4.target_order_ticket_sql,
}

hb_orders_date_sql = {
    'hb_orders_daily_sql': hb_orders_date_sql.hb_orders_daily,
    'update_hb_orders_daily_sql': hb_orders_date_sql.update_hb_orders_daily,

    'hb_gt_order_daily_sql': hb_order.hb_order_ticket_sql,
    'update_hb_gt_order_daily_sql': hb_order.update_hb_order_ticket_sql
}

hb_consumers_sql = {
    'hb_consumers_daily': hb_consumers.hb_consumers_daily,
    'update_hb_consumers_daily': hb_consumers.update_hb_consumers_daily,

    'hb_consumers_weekly': hb_consumers.hb_consumers_weekly,
    'update_hb_consumers_weekly': hb_consumers.update_hb_consumers_weekly,

    'hb_consumers_monthly': hb_consumers.hb_consumers_monthly,
    'update_hb_consumers_monthly': hb_consumers.update_hb_consumers_monthly,

    'hb_consumers_quarterly': hb_consumers.hb_consumers_quarterly,
    'update_hb_consumers_quarterly': hb_consumers.update_hb_consumers_quarterly,

    'hb_newconsumers_daily': hb_consumers.hb_newconsumers_daily,
    'update_hb_newconsumers_daily': hb_consumers.update_hb_newconsumers_daily,


}

hb_flight_detail_user_sql = {
    'hb_filght_detail_user_daily': hb_flight_details.hb_filght_detail_user_daily,
    'update_flight_detail_user_daily': hb_flight_details.update_flight_detail_user_daily,

    'hb_filght_detail_user_weekly': hb_flight_details.hb_filght_detail_user_weekly,
    'update_flight_detail_user_weekly': hb_flight_details.update_flight_detail_user_weekly,
    'hb_filght_detail_user_table_weekly': hb_flight_details.hb_filght_detail_user_table_weekly,

    'hb_filght_detail_user_monthly': hb_flight_details.hb_filght_detail_user_monthly,
    'update_flight_detail_user_monthly': hb_flight_details.update_flight_detail_user_monthly,

    'hb_filght_detail_user_quarterly': hb_flight_details.hb_filght_detail_user_quarterly,
    'update_flight_detail_user_quarterly': hb_flight_details.update_flight_detail_user_quarterly,

}

hb_flight_search_user_sql = {
    'hb_filght_search_user_daily': hb_flight_search.hb_filght_search_user_daily,
    'update_flight_search_user_daily': hb_flight_search.update_flight_search_user_daily,

    'hb_filght_search_user_weekly': hb_flight_search.hb_filght_search_user_weekly,
    'update_flight_search_user_weekly': hb_flight_search.update_flight_search_user_weekly,
    'hb_filght_search_user_table_weekly': hb_flight_search.hb_filght_search_user_table_weekly,

    'hb_filght_search_user_monthly': hb_flight_search.hb_filght_search_user_monthly,
    'update_flight_search_user_monthly': hb_flight_search.update_flight_search_user_monthly,

    'hb_filght_search_user_quarterly': hb_flight_search.hb_filght_search_user_quarterly,
    'update_flight_search_user_quarterly': hb_flight_search.update_flight_search_user_quarterly

}

hb_flight_focus_user_sql = {
    'hb_flight_focus_users_daily': hb_flight_focus.hb_flight_focus_users_daily,
    'update_flight_focus_user_daily': hb_flight_focus.update_flight_focus_user_daily,

    'hb_flight_focus_users_weekly': hb_flight_focus.hb_flight_focus_users_weekly,
    'update_flight_focus_user_weekly': hb_flight_focus.update_flight_focus_user_weekly,

    'hb_flight_focus_users_monthly': hb_flight_focus.hb_flight_focus_users_monthly,
    'update_flight_focus_user_monthly': hb_flight_focus.update_flight_focus_user_monthly,

    'hb_flight_focus_users_quarterly': hb_flight_focus.hb_flight_focus_users_quarterly,
    'update_flight_focus_user_quarterly': hb_flight_focus.update_flight_focus_user_quarterly
}

hb_activeusers_sql = {
    "hbgj_activeusers_daily": hb_activeusers.hbgj_activeusers_daily,
    "update_hbgj_activeusers_daily": hb_activeusers.update_hbgj_activeusers_daily,
    "hbgj_activeusers_weekly": hb_activeusers.hbgj_activeusers_weekly,
    "update_hbgj_activeusers_weekly": hb_activeusers.update_hbgj_activeusers_weekly,
    "hbgj_activeusers_monthly": hb_activeusers.hbgj_activeusers_monthly,
    "update_hbgj_activeusers_monthly": hb_activeusers.update_hbgj_activeusers_monthly,

}

coupon_sql = {
    "hbgj_use_coupon_sql": coupon_ticket.hbgj_use_coupon_sql,
    "hbgj_issue_coupon_sql": coupon_ticket.hbgj_issue_coupon_sql,
    "insert_hbgj_coupon_sql": coupon_ticket.insert_hbgj_coupon_sql,
    "gtgj_use_issue_coupon_sql": coupon_ticket.gtgj_use_issue_coupon_sql,
    "insert_gtgj_coupon_sql": coupon_ticket.insert_gtgj_coupon_sql,
    "huoli_car_issue_coupon_sql": coupon_ticket.huoli_car_issue_coupon_sql,
    "huoli_car_use_coupon_sql": coupon_ticket.huoli_car_use_coupon_sql,
    "insert_huoli_car_sql": coupon_ticket.insert_huoli_car_sql,

    "huoli_hotel_issue_coupon_sql": coupon_ticket.huoli_hotel_issue_coupon_sql,
    "huoli_hotel_use_coupon_sql": coupon_ticket.huoli_hotel_use_coupon_sql,
    "insert_huoli_hotel_sql": coupon_ticket.insert_huoli_hotel_sql,

    "common_coupon_sql": coupon_ticket.common_coupon_sql,
    "insert_common_coupon_sql": coupon_ticket.insert_common_coupon_sql,

    "hbdj_use_detail_sql": coupon_ticket.hbdj_use_detail_sql,
    "insert_hbgj_use_detail_sql": coupon_ticket.insert_hbgj_use_detail_sql,

    "coupon_issue_detail_sql": coupon_ticket.coupon_issue_detail_sql,
    "insert_coupon_issue_detail_sql": coupon_ticket.insert_coupon_issue_detail_sql,

    "huoli_car_coupon_detail_sql": coupon_ticket.huoli_car_coupon_detail_sql,
    "insert_coupon_car_use_detail_sql": coupon_ticket.insert_coupon_car_use_detail_sql,

    "huoli_hotel_use_detail_sql": coupon_ticket.huoli_hotel_use_detail_sql,
    "insert_huoli_hotel_use_detail_sql": coupon_ticket.insert_huoli_hotel_use_detail_sql,

    "gtgj_coupon_issue_detail_sql": coupon_ticket.gtgj_coupon_issue_detail_sql,
    "insert_gtgj_coupon_issue_detail_sql": coupon_ticket.insert_gtgj_coupon_issue_detail_sql,
    "gtgj_coupon_use_detail_sql": coupon_ticket.gtgj_coupon_use_detail_sql,
    "insert_gtgj_coupon_use_detail_sql": coupon_ticket.insert_gtgj_coupon_use_detail_sql,

}