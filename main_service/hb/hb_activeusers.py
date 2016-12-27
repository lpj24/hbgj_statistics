from sql.hb_sqlHandlers import hb_activeusers_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_hbgj_activeusers_daily(days=0):
    today = DateUtil.date2str(DateUtil.getDateBeforeDays(int(days)), '%Y-%m-%d')
    tomorrow = DateUtil.date2str(DateUtil.getDateAfterDays(1 - int(days)), '%Y-%m-%d')
    dto = {"s_day": today, "start_date": today, "end_date": tomorrow}
    query_data = DBCli().oracle_cli.queryOne(hb_activeusers_sql["hbgj_activeusers_daily"], dto)
    DBCli().targetdb_cli.insert(hb_activeusers_sql["update_hbgj_activeusers_daily"], query_data)


def update_hbgj_activeusers_weekly():
    start_date, end_date = DateUtil.getLastWeekDate()
    start_date = DateUtil.date2str(start_date, '%Y-%m-%d')
    end_date = DateUtil.date2str(end_date, '%Y-%m-%d')
    dto = {"start_date": start_date, "end_date": end_date}
    query_data = DBCli().oracle_cli.queryOne(hb_activeusers_sql["hbgj_activeusers_weekly"], dto)
    DBCli().targetdb_cli.insert(hb_activeusers_sql["update_hbgj_activeusers_weekly"], query_data)


def update_hbgj_activeusers_monthly():
    start_date, end_date = DateUtil.getThisMonthDate()
    start_date = DateUtil.date2str(start_date, '%Y-%m-%d')
    end_date = DateUtil.date2str(end_date, '%Y-%m-%d')
    dto = {"start_date": start_date, "end_date": end_date}
    query_data = DBCli().oracle_cli.queryOne(hb_activeusers_sql["hbgj_activeusers_monthly"], dto)
    DBCli().targetdb_cli.insert(hb_activeusers_sql["update_hbgj_activeusers_monthly"], query_data)

if __name__ == "__main__":
    # for x in xrange(6, 0, -1):
    update_hbgj_activeusers_daily(1)
    # update_hbgj_activeusers_weekly()
    # update_hbgj_activeusers_monthly()