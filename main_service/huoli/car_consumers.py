from sql.huoli_sqlHandlers import car_consumers_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_car_consumers_daily(days=0):
    today = DateUtil.getDateBeforeDays(int(days))
    tomorrow = DateUtil.getDateAfterDays(1-int(days))
    dto = []
    # for i in xrange(4):
    #     dto.append(DateUtil.date2str(today, '%Y-%m-%d'))
    #     dto.append(DateUtil.date2str(today))
    #     dto.append(DateUtil.date2str(tomorrow))
    #
    # query_data = car_cli.queryOne(car_consumers_sql['car_consumers_daily'], dto)
    # targetdb_cli.insert(car_consumers_sql['update_car_consumers_daily'], query_data)

    for i in xrange(3):
        dto.append(DateUtil.date2str(today, '%Y-%m-%d'))
        dto.append(DateUtil.date2str(today))
        dto.append(DateUtil.date2str(tomorrow))

    query_data = DBCli().car_cli.queryOne(car_consumers_sql["car_consumers_jz_daily"], dto)
    DBCli().targetdb_cli.insert(car_consumers_sql["update_car_consumers_jz_daily"], query_data)


def update_car_consumers_weekly():
    start_date, end_date = DateUtil.getLastWeekDate()
    dto = []
    # for i in xrange(4):
    #     dto.append(DateUtil.date2str(start_date, '%Y-%m-%d'))
    #     dto.append(DateUtil.date2str(start_date))
    #     dto.append(DateUtil.date2str(end_date))
    #
    # query_data = car_cli.queryOne(car_consumers_sql['car_consumers_weekly'], dto)
    # targetdb_cli.insert(car_consumers_sql['update_car_consumers_weekly'], query_data)

    for i in xrange(3):
        dto.append(DateUtil.date2str(start_date, '%Y-%m-%d'))
        dto.append(DateUtil.date2str(start_date))
        dto.append(DateUtil.date2str(end_date))

    query_data = DBCli().car_cli.queryOne(car_consumers_sql["car_consumers_jz_weekly"], dto)
    DBCli().targetdb_cli.insert(car_consumers_sql["update_car_consumers_jz_weekly"], query_data)


def update_car_consumers_monthly():
    start_date, end_date = DateUtil.getLastMonthDate()
    dto = []
    # for i in xrange(4):
    #     dto.append(DateUtil.date2str(start_date, '%Y-%m-%d'))
    #     dto.append(DateUtil.date2str(start_date))
    #     dto.append(DateUtil.date2str(end_date))
    # query_data = car_cli.queryOne(car_consumers_sql['car_consumers_monthly'], dto)
    # targetdb_cli.insert(car_consumers_sql['update_car_consumers_monthly'], query_data)

    for i in xrange(3):
        dto.append(DateUtil.date2str(start_date, '%Y-%m-%d'))
        dto.append(DateUtil.date2str(start_date))
        dto.append(DateUtil.date2str(end_date))
    query_data = DBCli().car_cli.queryOne(car_consumers_sql["car_consumers_jz_monthly"], dto)
    DBCli().targetdb_cli.insert(car_consumers_sql["update_car_consumers_jz_monthly"], query_data)


def update_car_consumers_quarterly():
    start_date, end_date = DateUtil.getLastQuarterDate()
    dto = []
    # for i in xrange(4):
    #     dto.append(DateUtil.date2str(start_date, '%Y-%m-%d'))
    #     dto.append(DateUtil.date2str(start_date, '%Y-%m-%d'))
    #     dto.append(DateUtil.date2str(start_date))
    #     dto.append(DateUtil.date2str(end_date))
    # query_data = car_cli.queryOne(car_consumers_sql['car_consumers_quarterly'], dto)
    # targetdb_cli.insert(car_consumers_sql['update_car_consumers_quarterly'], query_data)

    for i in xrange(3):
        dto.append(DateUtil.date2str(start_date, '%Y-%m-%d'))
        dto.append(DateUtil.date2str(start_date, '%Y-%m-%d'))
        dto.append(DateUtil.date2str(start_date))
        dto.append(DateUtil.date2str(end_date))
    query_data = DBCli().car_cli.queryOne(car_consumers_sql["car_consumers_jz_quarterly"], dto)
    DBCli().targetdb_cli.insert(car_consumers_sql["update_car_consumers_jz_quarterly"], query_data)


def update_car_newconsumers_daily(days=0):
    today = DateUtil.getDateBeforeDays(int(days))
    dto = []
    for i in xrange(3):
        dto.append(DateUtil.date2str(today, '%Y-%m-%d'))
        dto.append(DateUtil.date2str(today, '%Y-%m-%d'))
        dto.append(DateUtil.date2str(today, '%Y-%m-%d'))
    query_data = DBCli().car_cli.queryOne(car_consumers_sql['car_newconsumers_daily'], dto)
    DBCli().targetdb_cli.insert(car_consumers_sql['update_car_newconsumers_daily'], query_data)

if __name__ == "__main__":
    update_car_consumers_daily(2)
    # update_car_consumers_weekly()
    # for i in xrange(4, 0, -1):
    #     update_car_newconsumers_daily(i)
    # update_car_consumers_monthly()
    # update_car_consumers_quarterly()
    # update_car_newconsumers_daily()