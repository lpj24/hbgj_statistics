
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