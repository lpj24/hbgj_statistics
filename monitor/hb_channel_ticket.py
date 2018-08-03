# -*- coding: utf-8 -*-
from dbClient.dateutil import DateUtil
from dbClient.db_client import DBCli
import datetime
from collections import defaultdict


def monitor_hb_channel_ticket_hourly():
    now_hour_sql = """
        SELECT DATE_FORMAT(od.CREATETIME, '%%Y-%%m-%%d') s_day,o.PNRSOURCE,c.NAME
        as 渠道,count(*) as 票量 FROM `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        join `PNRSOURCE_CONFIG` c on o.PNRSOURCE=c.PNRSOURCE
        where
        od.CREATETIME >= %s
        and od.CREATETIME <= %s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
        IFNULL(od.`LINKTYPE`, 0) != 2 GROUP BY s_day, o.PNRSOURCE;
    """

    s_day = DateUtil.get_today("%Y-%m-%d")
    # s_hour = int(datetime.datetime.now().strftime("%H"))
    # s_hour = xrange(0, 24)
    for s_hour in xrange(8, 22):
        s_day = DateUtil.date2str(DateUtil.get_date_before_days(1), '%Y-%m-%d')
        if s_hour == 0:
            # s_day = DateUtil.date2str(DateUtil.get_date_before_days(1), '%Y-%m-%d')
            s_hour = 23
        else:
            s_hour -= 1
        start_date = s_day + " " + str(s_hour) + ":00:00"
        end_date = s_day + " " + str(s_hour) + ":59:59"
        dto = [start_date, end_date]
        print dto
        avg_hourly_data = get_three_week_same_hourly_data(now_hour_sql, s_hour)
        now_hour_data = DBCli().sourcedb_cli.query_all(now_hour_sql, dto)
        total_hour_num = {}
        total_ticket = 0
        for data in now_hour_data:
            s_day, pn, pn_name, ticket_num = data
            total_hour_num[pn + ':' + pn_name] = ticket_num
            total_ticket += ticket_num
            if avg_hourly_data.get(pn + ':' + pn_name, 0) > ticket_num:
                print pn, pn_name, ticket_num, avg_hourly_data.get(pn + ':' + pn_name, 0)


def get_three_week_same_hourly_data(sql, s_hour):
    timedelta_day = 7
    query_hour = [s_hour] * 3
    three_week_data = defaultdict(int)
    for index, h in enumerate(query_hour):
        query_day = DateUtil.get_date_before_days(timedelta_day * (index + 1))
        start_date = str(query_day) + " " + str(h) + ":00:00"
        end_date = str(query_day) + " " + str(h) + ":59:59"
        query_data = DBCli().sourcedb_cli.query_all(sql, [start_date, end_date])
        map_channel_ticket(query_data, three_week_data)

    return {k: float(v/3) * 0.8 for k, v in three_week_data.items()}


def map_channel_ticket(query_data, total_dict={}):
    for data in query_data:
        s_day, pn, pn_name, ticket_num = data
        total_dict[pn + ':' + pn_name] += ticket_num


if __name__ == '__main__':
    monitor_hb_channel_ticket_hourly()
# get_three_week_same_hourly_data()