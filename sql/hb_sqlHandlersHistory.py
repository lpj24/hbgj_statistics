from sql.hbsql.history import hb_flight_details_history, hb_flight_focus_history, hb_flight_search_history

hb_flight_detail_user_history_sql = {
    'hb_filght_detail_user_daily_history': hb_flight_details_history.hb_filght_detail_user_daily_history,
    'update_flight_detail_user_daily_history': hb_flight_details_history.update_flight_detail_user_daily_history,

    'hb_filght_detail_user_weeky_history': hb_flight_details_history.hb_filght_detail_user_weekly_history,
    'hb_filght_detail_user_difftable_weekly': hb_flight_details_history.hb_filght_detail_user_difftable_weekly,
    'update_flight_detail_user_weekly_history': hb_flight_details_history.update_flight_detail_user_weekly_history,

    'update_flight_detail_user_monthly_history': hb_flight_details_history.update_flight_detail_user_monthly_history,
    'hb_filght_detail_user_monthly_history': hb_flight_details_history.hb_filght_detail_user_monthly_history,

    'hb_filght_detail_user_quarterly_history': hb_flight_details_history.hb_filght_detail_user_quarterly_history,
    'update_flight_detail_user_quarterly_history': hb_flight_details_history.update_flight_detail_user_quarterly_history,

    "flight_detail_user_query_daily_his": hb_flight_details_history.flight_detail_user_query_daily_his,
    "update_flight_detail_user_query_daily_his": hb_flight_details_history.update_flight_detail_user_query_daily_his,

    "update_flight_detail_user_query_weekly_his": hb_flight_details_history.update_flight_detail_user_query_weekly_his,
    "update_flight_detail_user_query_monthly_his": hb_flight_details_history.update_flight_detail_user_query_monthly_his,
    "update_flight_detail_user_query_quarterly_his": hb_flight_details_history.update_flight_detail_user_query_quarterly_his,
}


hb_flight_focus_user_history_sql = {
    'hb_flight_focus_users_daily_history': hb_flight_focus_history.hb_flight_focus_users_daily_history,
    'update_flight_focus_user_daily_history': hb_flight_focus_history.update_flight_focus_user_daily_history,

    'hb_flight_focus_users_weekly_history': hb_flight_focus_history.hb_flight_focus_users_weekly_history,
    'update_flight_focus_user_weekly_history': hb_flight_focus_history.update_flight_focus_user_weekly_history,

    'hb_flight_focus_users_monthly_history': hb_flight_focus_history.hb_flight_focus_users_monthly_history,
    'update_flight_focus_user_monthly_history': hb_flight_focus_history.update_flight_focus_user_monthly_history,

    'hb_flight_focus_users_quarterly_history': hb_flight_focus_history.hb_flight_focus_users_quarterly_history,
    'update_flight_focus_user_quarterly_history': hb_flight_focus_history.update_flight_focus_user_quarterly_history,

}

hb_flight_search_user_history_sql = {
    'hb_filght_search_user_daily_history': hb_flight_search_history.hb_filght_search_user_daily_history,
    'update_flight_search_user_daily_history': hb_flight_search_history.update_flight_search_user_daily_history,

    'hb_filght_search_user_weeky_history': hb_flight_search_history.hb_filght_search_user_weekly_history,
    'hb_filght_search_user_difftable_weekly': hb_flight_search_history.hb_filght_search_user_difftable_weekly,
    'update_flight_search_user_weekly_history': hb_flight_search_history.update_flight_search_user_weekly_history,

    'update_flight_search_user_monthly_history': hb_flight_search_history.update_flight_search_user_monthly_history,
    'hb_filght_search_user_monthly_history': hb_flight_search_history.hb_filght_search_user_monthly_history,

    'hb_filght_search_user_quarterly_history': hb_flight_search_history.hb_filght_search_user_quarterly_history,
    'update_flight_search_user_quarterly_history': hb_flight_search_history.update_flight_search_user_quarterly_history,

    "update_flight_search_user_pv_daily_history": hb_flight_search_history.update_flight_search_user_pv_daily_history,
    "update_flight_search_user_pv_weekly_history": hb_flight_search_history.update_flight_search_user_pv_weekly_history,

    "update_flight_search_user_pv_monthly_history": hb_flight_search_history.update_flight_search_user_pv_monthly_history,
    "update_flight_search_user_pv_quarterly_history": hb_flight_search_history.update_flight_search_user_pv_quarterly_history,
}
