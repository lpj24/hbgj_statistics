from sql.huolisql import eat_activeusers, car_orders, car_consumers, hotel_newconsumers, \
    hotel_order, hotel_consumers
eat_activeusers_sql = {
    "eat_activeusers_daily": eat_activeusers.eat_activeusers_daily,
    "insert_eat_activeusers_daily": eat_activeusers.insert_eat_activeusers_daily,

    "eat_activeusers_weekly": eat_activeusers.eat_activeusers_weekly,
    "insert_eat_activeusers_weekly": eat_activeusers.insert_eat_activeusers_weekly,

    "eat_activeusers_monthly": eat_activeusers.eat_activeusers_monthly,
    "insert_eat_activeusers_monthly": eat_activeusers.insert_eat_activeusers_monthly,

    "eat_activeusers_quarterly": eat_activeusers.eat_activeusers_quarterly,
    "insert_eat_activeusers_quarterly": eat_activeusers.insert_eat_activeusers_quarterly,
}

car_orders_sql = {
    "car_orders_daily": car_orders.car_orders_daily,
    "update_car_orders_daily": car_orders.update_car_orders_daily,
    "car_orders_jz_daily": car_orders.car_orders_jz_daily,
    "update_car_orders_jz_daily": car_orders.update_car_orders_jz_daily,
}

car_consumers_sql = {
    "car_consumers_daily": car_consumers.car_consumers_daily,
    "update_car_consumers_daily": car_consumers.update_car_consumers_daily,

    "car_consumers_weekly": car_consumers.car_consumers_weekly,
    "update_car_consumers_weekly": car_consumers.update_car_consumers_weekly,

    "car_consumers_monthly": car_consumers.car_consumers_monthly,
    "update_car_consumers_monthly": car_consumers.update_car_consumers_monthly,

    "car_consumers_quarterly": car_consumers.car_consumers_quarterly,
    "update_car_consumers_quarterly": car_consumers.update_car_consumers_quarterly,

    "car_newconsumers_daily": car_consumers.car_newconsumers_daily,
    "update_car_newconsumers_daily": car_consumers.update_car_newconsumers_daily,

    "car_consumers_jz_daily": car_consumers.car_consumers_jz_daily,
    "update_car_consumers_jz_daily": car_consumers.update_car_consumers_jz_daily,
    "car_consumers_jz_weekly": car_consumers.car_consumers_jz_weekly,
    "update_car_consumers_jz_weekly": car_consumers.update_car_consumers_jz_weekly,
    "car_consumers_jz_monthly": car_consumers.car_consumers_jz_monthly,
    "update_car_consumers_jz_monthly": car_consumers.update_car_consumers_jz_monthly,
    "car_consumers_jz_quarterly": car_consumers.car_consumers_jz_quarterly,
    "update_car_consumers_jz_quarterly": car_consumers.update_car_consumers_jz_quarterly,
}

hotel_newconsumers_sql = {
    "hotel_newconsumers_daily": hotel_newconsumers.hotel_newconsumers_daily,
    "update_hotel_newconsumers_daily": hotel_newconsumers.update_hotel_newconsumers_daily,
    "hotel_newconsumers_p2p_daily": hotel_newconsumers.hotel_newconsumers_p2p_daily,
}

hotel_orders_sql = {
    "hotel_orders_daily": hotel_order.hotel_orders_daily,
    "update_hotel_orders_daily": hotel_order.update_hotel_orders_daily,
    "hotel_orders_daily_history": hotel_order.hotel_orders_daily_history,
}

hotel_consumers_sql = {
    "hotel_consumers_daily": hotel_consumers.hotel_consumers_daily,
    "hotel_consumers_daily_his": hotel_consumers.hotel_consumers_daily_his,
    "update_hotel_consumers_daily": hotel_consumers.update_hotel_consumers_daily,

    "hotel_consumers_weekly": hotel_consumers.hotel_consumers_weekly,
    "hotel_consumers_weekly_his": hotel_consumers.hotel_consumers_weekly_his,
    "update_hotel_consumers_weekly": hotel_consumers.update_hotel_consumers_weekly,

    "hotel_consumers_monthly": hotel_consumers.hotel_consumers_monthly,
    "hotel_consumers_monthly_his": hotel_consumers.hotel_consumers_monthly_his,
    "update_hotel_consumers_monthly": hotel_consumers.update_hotel_consumers_monthly,
}