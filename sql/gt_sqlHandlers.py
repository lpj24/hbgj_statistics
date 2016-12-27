from gtsql import gtgj_activeusers, gtgj_consumers, gt_order, gtgj_amount, gt_neworder

gtgj_activeusers_sql = {
    "gtgj_activeusers_daily": gtgj_activeusers.gtgj_activeusers_daily,
    "update_gtgj_activeusers_daily": gtgj_activeusers.update_gtgj_activeusers_daily,

    "gtgj_activeusers_weekly": gtgj_activeusers.gtgj_activeusers_weekly,
    "update_gtgj_activeusers_weekly": gtgj_activeusers.update_gtgj_activeusers_weekly,

    "gtgj_activeusers_monthly": gtgj_activeusers.gtgj_activeusers_monthly,
    "update_gtgj_activeusers_monthly": gtgj_activeusers.update_gtgj_activeusers_monthly,

    "update_gtgj_activeusers_quarterly": gtgj_activeusers.update_gtgj_activeusers_quarterly,

    "gtgj_newusers_daily": gtgj_activeusers.gtgj_newusers_daily,
    "update_gtgj_newusers_daily": gtgj_activeusers.update_gtgj_newusers_daily,

}

gtgj_consumers_sql = {
    "gtgj_consumers_daily": gtgj_consumers.gtgj_consumers_daily,
    "update_gtgj_consumers_daily": gtgj_consumers.update_gtgj_consumers_daily,

    "gtgj_consumers_weekly": gtgj_consumers.gtgj_consumers_weekly,
    "update_gtgj_consumers_weekly": gtgj_consumers.update_gtgj_consumers_weekly,

    "gtgj_consumers_monthly": gtgj_consumers.gtgj_consumers_monthly,
    "update_gtgj_consumers_monthly": gtgj_consumers.update_gtgj_consumers_monthly,

    "gtgj_consumers_quarterly": gtgj_consumers.gtgj_consumers_quarterly,
    "update_gtgj_consumers_quarterly": gtgj_consumers.update_gtgj_consumers_quarterly,
}

gt_order_sql = {
    "gtgj_order_daily": gt_order.gtgj_order_daily,
    "gtgj_order_daily_his": gt_order.gtgj_order_daily_his,
    "update_gtgj_order_daily": gt_order.update_gtgj_order_daily,

    "gtgj_order_hourly": gt_order.gtgj_order_hourly,
    "query_gtgj_order_by_hour": gt_order.query_gtgj_order_by_hour,
    "insert_gtgj_order_hourly": gt_order.insert_gtgj_order_hourly,
    "update_gtgj_order_hourly": gt_order.update_gtgj_order_hourly,

    "gtgj_order_from_hb": gt_order.query_gtgj_order_from_hb,
    "gtgj_ticket_from_hb": gt_order.query_gtgj_ticket_from_hb,
    "insert_gtgj_from_hb": gt_order.insert_gtgj_from_hb

}

gt_amount_sql = {
    "gtgj_amount_create": gtgj_amount.gtgj_amount_create,
    "gtgj_amount_success": gtgj_amount.gtgj_amount_success,
    "gtgj_amount_success_daily": gtgj_amount.gtgj_amount_success_daily,
    "gtgj_change_oids": gtgj_amount.gtgj_change_oids,
    "gtgj_change_info": gtgj_amount.gtgj_change_info,
    "gtgj_amount_grab": gtgj_amount.gtgj_amount_grab,
    "update_gtgj_amount_daily": gtgj_amount.update_gtgj_amount_daily,
}

gt_new_order_sql = {
    "gt_neworder_daily": gt_neworder.gt_neworder_daily,
    "update_gtgj_new_order_daily": gt_neworder.update_gtgj_new_order_daily,

}