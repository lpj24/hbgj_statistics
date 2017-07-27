# -*- coding: utf-8 -*-
from sql.hb_sqlHandlers import hb_consumers_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_hb_consumers_daily(days=0):
    """更新航班消费用户, hbgj_consumers_daily"""
    today_date = DateUtil.date2str(DateUtil.get_date_before_days(int(days)), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)), '%Y-%m-%d')
    dto = [today_date, end_date, today_date, end_date]
    query_data = DBCli().sourcedb_cli.queryOne(hb_consumers_sql['hb_consumers_daily'], dto)
    DBCli().targetdb_cli.insert(hb_consumers_sql['update_hb_consumers_daily'], query_data)
    return __file__


def update_hb_consumers_weekly(days=0):
    """更新航班消费周用户, hbgj_consumers_weekly_test"""
    start_weekdate, end_weekdate = DateUtil.get_this_week_date()
    start_date = DateUtil.date2str(start_weekdate)
    end_date = DateUtil.date2str(end_weekdate)
    dto = [start_date, end_date, start_date, end_date]
    query_data = DBCli().sourcedb_cli.queryOne(hb_consumers_sql['hb_consumers_weekly'], dto)
    DBCli().targetdb_cli.insert(hb_consumers_sql['update_hb_consumers_weekly'], query_data)
    return __file__


def update_hb_consumers_monthly(days=0):
    """更新航班月消费用户, hbgj_consumers_monthly_test"""
    start_monthdate, end_enddate = DateUtil.get_this_month_date()
    start_monthdate = DateUtil.date2str(start_monthdate)
    end_enddate = DateUtil.date2str(end_enddate)
    dto = [start_monthdate, end_enddate, start_monthdate, end_enddate]
    query_data = DBCli().sourcedb_cli.queryOne(hb_consumers_sql['hb_consumers_monthly'], dto)
    DBCli().targetdb_cli.insert(hb_consumers_sql['update_hb_consumers_monthly'], query_data)
    return __file__


def update_hb_consumers_quarterly():
    start_quarterdate, end_quarterdate = DateUtil.get_last_quarter_date()
    start_quarterdate = DateUtil.date2str(start_quarterdate)
    end_quarterdate = DateUtil.date2str(end_quarterdate)
    dto = [start_quarterdate, end_quarterdate, start_quarterdate, end_quarterdate]
    query_data = DBCli().sourcedb_cli.queryOne(hb_consumers_sql['hb_consumers_quarterly'], dto)
    DBCli().targetdb_cli.insert(hb_consumers_sql['update_hb_consumers_daily'], query_data)


def update_hb_newconsumers_daily(days=0):
    """更新航班新增消费用户, hbgj_newconsumers_daily"""
    yesterday = DateUtil.get_before_days(days)
    dto = [yesterday, yesterday, yesterday, yesterday]
    query_data = DBCli().sourcedb_cli.queryOne(hb_consumers_sql['hb_newconsumers_daily'], dto)
    DBCli().targetdb_cli.insert(hb_consumers_sql['update_hb_newconsumers_daily'], query_data)
    return __file__


def update_hbgj_newconsumers_inter_daily(days=0):
    """更新国际机票新增消费用户, hbgj_newconsumers_inter_daily"""
    today = DateUtil.date2str(DateUtil.get_date_before_days(int(days) * 1), '%Y-%m-%d')
    tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)), '%Y-%m-%d')
    dto = [today, tomorrow, today, today]
    inter_sql = """
        select A.s_day, A.consumers, A.consumers_ios, (A.consumers - A.consumers_ios) from (
        SELECT DATE_FORMAT(od.CREATETIME, '%%Y-%%m-%%d') s_day,count(DISTINCT PHONEID) consumers,
        count(distinct case when p LIKE '%%ios%%' then PHONEID else null end ) consumers_ios
        FROM `TICKET_ORDERDETAIL` od
        INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        where od.CREATETIME>= %s
        and od.CREATETIME < %s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        AND IFNULL(od.`LINKTYPE`, 0) != 2
        and INTFLAG=1
        and phoneid not in (
            SELECT distinct PHONEID
            FROM `TICKET_ORDERDETAIL` od
            INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
            where  o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
            AND IFNULL(od.`LINKTYPE`, 0) != 2
            and INTFLAG=1
            and od.CREATETIME< %s
        )
        and phoneid in (
            SELECT distinct PHONEID
            FROM `TICKET_ORDERDETAIL` od
            INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
            where  o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
            AND IFNULL(od.`LINKTYPE`, 0) != 2
            and INTFLAG=0
            and od.CREATETIME<%s
        )

        ) A
    """
    insert_sql = """
        insert into hbgj_newconsumers_inter_daily (s_day, new_consumers, new_consumers_ios, new_consumers_android,
        createtime, updatetime)
        values
        (%s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        new_consumers = values(new_consumers),
        new_consumers_ios = values(new_consumers_ios),
        new_consumers_android = values(new_consumers_android)
    """
    query_data = DBCli().sourcedb_cli.queryAll(inter_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_sql, query_data)


def update_hbgj_consumers_inter_daily(days=0):
    """更新国际机票消费用户, hbgj_consumers_inter_daily"""
    today = DateUtil.date2str(DateUtil.get_date_before_days(int(days) * 3), '%Y-%m-%d')
    tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)), '%Y-%m-%d')
    dto = [today, tomorrow]
    inter_sql = """
        select A.s_day, A.consumers, A.consumers_ios, (A.consumers - A.consumers_ios) from (
        SELECT DATE_FORMAT(od.CREATETIME, '%%Y-%%m-%%d') s_day,count(DISTINCT PHONEID) consumers,
        count(distinct case when p LIKE '%%ios%%' then PHONEID else null end ) consumers_ios
        FROM `TICKET_ORDERDETAIL` od
        INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        where od.CREATETIME>= %s
        and od.CREATETIME < %s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        AND IFNULL(od.`LINKTYPE`, 0) != 2
        and INTFLAG=1
        group by s_day
        ) A
    """
    insert_sql = """
        insert into hbgj_consumers_inter_daily (s_day, consumers, consumers_ios, consumers_android,
        createtime, updatetime)
        values
        (%s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        consumers = values(consumers),
        consumers_ios = values(consumers_ios),
        consumers_android = values(consumers_android)
    """
    query_data = DBCli().sourcedb_cli.queryAll(inter_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_sql, query_data)

if __name__ == "__main__":
    i = 1
    while i <= 1708:
        update_hbgj_newconsumers_inter_daily(i)
        i += 1
    # update_hbgj_consumers_inter_daily(1)
    # update_hb_newconsumers_daily(1)
    # update_hb_consumers_weekly()
    # update_hb_consumers_monthly()
    # update_hb_consumers_quarterly()