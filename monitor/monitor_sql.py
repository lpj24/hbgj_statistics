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

]