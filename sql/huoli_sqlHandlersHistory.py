from sql.huolisql.history import eat_activeusers_history, car_orders_history

eat_activeusers_daily_history_sql = {
    'eat_activeusers_daily_history': eat_activeusers_history.eat_activeusers_daily_history,
    'insert_eat_activeusers_daily_history': eat_activeusers_history.insert_eat_activeusers_daily_history,

    'eat_activeusers_monthly_history': eat_activeusers_history.eat_activeusers_monthly_history,
    'insert_eat_activeusers_monthly_history': eat_activeusers_history.insert_eat_activeusers_monthly_history,

    'eat_activeusers_weekly_history': eat_activeusers_history.eat_activeusers_weekly_history,
    'insert_eat_activeusers_weekly_history': eat_activeusers_history.insert_eat_activeusers_weekly_history,

    'eat_activeusers_quarterly_history': eat_activeusers_history.eat_activeusers_quarterly_history,
    'insert_eat_activeusers_quarterly_history': eat_activeusers_history.insert_eat_activeusers_quarterly_history,
}

car_history = {
    "car_orders_daily_history": car_orders_history.car_orders_daily_history,
    "insert_car_orders_daily_history": car_orders_history.insert_car_orders_daily_history,

    "car_consumers_daily_history": car_orders_history.car_consumers_daily_history,
    "insert_car_consumers_daily_history": car_orders_history.insert_car_consumers_daily_history,

    "car_newconsumers_daily_history": car_orders_history.car_newconsumers_daily_history,
    "insert_car_newconsumers_daily_history": car_orders_history.insert_car_newconsumers_daily_history,

    "car_consumers_weekly_history": car_orders_history.car_consumers_weekly_history,
    "insert_car_consumers_weekly_history": car_orders_history.insert_car_consumers_weekly_history,

    "car_consumers_monthly_history": car_orders_history.car_consumers_monthly_history,
    "insert_car_consumers_monthly_history": car_orders_history.insert_car_consumers_monthly_history,

    "car_consumers_quarterly_history": car_orders_history.car_consumers_quarterly_history,
    "insert_car_consumers_quarterly_history": car_orders_history.insert_car_consumers_quarterly_history,

    "car_orders_jz_daily_history": car_orders_history.car_orders_jz_daily_history,
    "insert_car_orders_jz_daily_history": car_orders_history.insert_car_orders_jz_daily_history,
    "car_consumers_jz_daily_history": car_orders_history.car_consumers_jz_daily_history,
    "insert_car_consumers_jz_daily_history": car_orders_history.insert_car_consumers_jz_daily_history,
    "car_consumers_jz_weekly_history": car_orders_history.car_consumers_jz_weekly_history,
    "insert_car_consumers_jz_weekly_history": car_orders_history.insert_car_consumers_jz_weekly_history,
    "car_consumers_jz_monthly_history": car_orders_history.car_consumers_jz_monthly_history,
    "insert_car_consumers_jz_monthly_history": car_orders_history.insert_car_consumers_jz_monthly_history,
    "car_consumers_jz_quarterly_history": car_orders_history.car_consumers_jz_quarterly_history,
    "insert_car_consumers_jz_quarterly_history": car_orders_history.insert_car_consumers_jz_quarterly_history,
}