from sql.hb_sqlHandlers import hb_flight_focus_user_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_flight_focus_user_daily(days=0):
    today = DateUtil.getDateBeforeDays(int(days))
    tomorrow = DateUtil.getDateAfterDays(1-int(days))
    dto = {"s_day": DateUtil.date2str(today, '%Y-%m-%d'), "start_date": DateUtil.date2str(today, '%Y-%m-%d'),
           "end_date":  DateUtil.date2str(tomorrow, '%Y-%m-%d')}
    query_date = DBCli().oracle_cli.queryOne(hb_flight_focus_user_sql['hb_flight_focus_users_daily'], dto)
    pv_sql = """
        SELECT count(*) FROM FLY_USERFOCUS_TBL
        where CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
        and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
    """
    pv_his_sql = """
        SELECT count(*) FROM FLY_USERFOCUS_TBL_HIS
        where CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
        and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
    """
    query_pv = DBCli().oracle_cli.queryOne(pv_sql, {"start_date": DateUtil.date2str(today, '%Y-%m-%d'), "end_date": DateUtil.date2str(tomorrow, '%Y-%m-%d')})
    query_his_pv = DBCli().oracle_cli.queryOne(pv_his_sql, {"start_date": DateUtil.date2str(today, '%Y-%m-%d'), "end_date": DateUtil.date2str(tomorrow, '%Y-%m-%d')})
    query_date = (query_date[0], query_date[1], int(query_pv[0]) + int(query_his_pv[0]))

    DBCli().targetdb_cli.insert(hb_flight_focus_user_sql['update_flight_focus_user_daily'], query_date)


def update_flight_focus_user_weekly():
    start_date, end_date = DateUtil.getLastWeekDate()
    start_date = DateUtil.date2str(start_date, '%Y-%m-%d')
    end_date = DateUtil.date2str(end_date, '%Y-%m-%d')
    dto = {"s_day": start_date, "start_date": start_date, "end_date": end_date}
    query_date = DBCli().oracle_cli.queryOne(hb_flight_focus_user_sql['hb_flight_focus_users_weekly'], dto)
    DBCli().targetdb_cli.insert(hb_flight_focus_user_sql['update_flight_focus_user_weekly'], query_date)


def update_flight_focus_user_monthly():
    start_date, end_date = DateUtil.getLastMonthDate()
    start_date = DateUtil.date2str(start_date, '%Y-%m-%d')
    end_date = DateUtil.date2str(end_date, '%Y-%m-%d')
    dto = {"s_day": start_date, "start_date": start_date, "end_date": end_date}
    query_date = DBCli().oracle_cli.queryOne(hb_flight_focus_user_sql['hb_flight_focus_users_monthly'], dto)
    print query_date
    DBCli().targetdb_cli.insert(hb_flight_focus_user_sql['update_flight_focus_user_monthly'], query_date)


def update_flight_focus_user_quarterly():
    start_date, end_date = DateUtil.getLastQuarterDate()
    start_date = DateUtil.date2str(start_date, '%Y-%m-%d')
    end_date = DateUtil.date2str(end_date, '%Y-%m-%d')
    dto = {"s_day": start_date, "start_date": start_date, "end_date": end_date}
    query_date = DBCli().oracle_cli.queryOne(hb_flight_focus_user_sql['hb_flight_focus_users_quarterly'], dto)
    DBCli().targetdb_cli.insert(hb_flight_focus_user_sql['update_flight_focus_user_quarterly'], query_date)


if __name__ == "__main__":
    update_flight_focus_user_daily(1)