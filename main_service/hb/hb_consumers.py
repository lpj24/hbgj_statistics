# -*- coding: utf-8 -*-
from sql.hb_sqlHandlers import hb_consumers_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_hb_consumers_daily(days=0):
    """航班消费用户, hbgj_consumers_daily"""
    today_date = DateUtil.date2str(DateUtil.get_date_before_days(int(days)), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)), '%Y-%m-%d')
    dto = [today_date, end_date, today_date, end_date]
    query_data = DBCli().sourcedb_cli.query_one(hb_consumers_sql['hb_consumers_daily'], dto)
    DBCli().targetdb_cli.insert(hb_consumers_sql['update_hb_consumers_daily'], query_data)


def update_hb_consumers_weekly(days=0):
    """航班消费周用户, hbgj_consumers_weekly_test"""
    start_weekdate, end_weekdate = DateUtil.get_this_week_date()
    start_date = DateUtil.date2str(start_weekdate)
    end_date = DateUtil.date2str(end_weekdate)
    dto = [start_date, end_date, start_date, end_date]
    query_data = DBCli().sourcedb_cli.query_one(hb_consumers_sql['hb_consumers_weekly'], dto)
    DBCli().targetdb_cli.insert(hb_consumers_sql['update_hb_consumers_weekly'], query_data)
    pass


def update_hb_consumers_monthly(days=0):
    """航班月消费用户, hbgj_consumers_monthly_test"""
    start_monthdate, end_enddate = DateUtil.get_this_month_date()
    start_monthdate = DateUtil.date2str(start_monthdate)
    end_enddate = DateUtil.date2str(end_enddate)
    dto = [start_monthdate, end_enddate, start_monthdate, end_enddate]
    query_data = DBCli().sourcedb_cli.query_one(hb_consumers_sql['hb_consumers_monthly'], dto)
    DBCli().targetdb_cli.insert(hb_consumers_sql['update_hb_consumers_monthly'], query_data)


def update_hb_consumers_quarterly():
    start_quarterdate, end_quarterdate = DateUtil.get_last_quarter_date()
    start_quarterdate = DateUtil.date2str(start_quarterdate)
    end_quarterdate = DateUtil.date2str(end_quarterdate)
    dto = [start_quarterdate, end_quarterdate, start_quarterdate, end_quarterdate]
    query_data = DBCli().sourcedb_cli.query_one(hb_consumers_sql['hb_consumers_quarterly'], dto)
    DBCli().targetdb_cli.insert(hb_consumers_sql['update_hb_consumers_daily'], query_data)


def update_hb_newconsumers_daily(days=0):
    """航班新增消费用户, hbgj_newconsumers_daily"""
    yesterday = DateUtil.get_before_days(days)
    dto = [yesterday, yesterday, yesterday, yesterday]
    query_data = DBCli().sourcedb_cli.query_one(hb_consumers_sql['hb_newconsumers_daily'], dto)
    DBCli().targetdb_cli.insert(hb_consumers_sql['update_hb_newconsumers_daily'], query_data)


def update_hbgj_newconsumers_inter_daily(days=0):
    """国际机票新增消费用户, hbgj_newconsumers_inter_daily"""
    today = DateUtil.date2str(DateUtil.get_date_before_days(int(days) * 1), '%Y-%m-%d')
    tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)), '%Y-%m-%d')
    dto = [today, tomorrow, today]
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
    query_data = DBCli().sourcedb_cli.query_all(inter_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)
    pass


def update_hbgj_consumers_inter_daily(days=0):
    """国际机票消费用户, hbgj_consumers_inter_daily"""
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
    query_data = DBCli().sourcedb_cli.query_all(inter_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)


def update_hbgj_consumers_inter_weekly():
    """国际机票消费用户(weekly), hbgj_consumers_inter_weekly"""
    start_date, end_date = DateUtil.get_last_week_date()
    dto = [start_date, end_date]
    inter_sql = """
        select A.s_day,
        A.consumers, A.consumers_ios, (A.consumers - A.consumers_ios) from (
        SELECT date_format(subdate(od.CREATETIME,date_format(od.CREATETIME,'%%w')-1),'%%Y-%%m-%%d') s_day,count(DISTINCT PHONEID) consumers,
        count(distinct case when p LIKE '%%ios%%' then PHONEID else null end ) consumers_ios
        FROM `TICKET_ORDERDETAIL` od
        INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        where od.CREATETIME>= %s
        and od.CREATETIME < %s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        AND IFNULL(od.`LINKTYPE`, 0) != 2
        and INTFLAG=1
        ) A
    """
    insert_sql = """
        insert into hbgj_consumers_inter_weekly (s_day, consumers, consumers_ios, consumers_android,
        createtime, updatetime)
        values
        (%s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        consumers = values(consumers),
        consumers_ios = values(consumers_ios),
        consumers_android = values(consumers_android)
    """
    query_data = DBCli().sourcedb_cli.query_all(inter_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)


def update_hbgj_consumers_inter_monthly():
    """国际机票消费用户(monthly), hbgj_consumers_inter_monthly"""
    start_date, end_date = DateUtil.get_last_month_date()
    dto = [start_date, end_date]
    inter_sql = """
        select A.s_day,
        A.consumers, A.consumers_ios, (A.consumers - A.consumers_ios) from (
        SELECT DATE_FORMAT(str_to_date(CONCAT(YEAR(od.createtime),'-',MONTH(od.createtime),'-01'),'%%Y-%%m-%%d'),'%%Y-%%m-%%d')
         s_day,count(DISTINCT PHONEID) consumers,
        count(distinct case when p LIKE '%%ios%%' then PHONEID else null end ) consumers_ios
        FROM `TICKET_ORDERDETAIL` od
        INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        where od.CREATETIME>= %s
        and od.CREATETIME < %s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        AND IFNULL(od.`LINKTYPE`, 0) != 2
        and INTFLAG=1
        ) A
    """
    insert_sql = """
        insert into hbgj_consumers_inter_monthly (s_day, consumers, consumers_ios, consumers_android,
        createtime, updatetime)
        values
        (%s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        consumers = values(consumers),
        consumers_ios = values(consumers_ios),
        consumers_android = values(consumers_android)
    """
    query_data = DBCli().sourcedb_cli.query_all(inter_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)


def update_hbgj_consumers_inter_quarterly():
    """国际机票消费用户(quarterly), hbgj_consumers_inter_quarterly"""
    start_date, end_date = DateUtil.get_last_quarter_date()
    dto = [DateUtil.date2str(start_date, '%Y-%m-%d'), DateUtil.date2str(start_date, '%Y-%m-%d'), start_date, end_date]
    inter_sql = """
        select A.s_day,
        A.consumers, A.consumers_ios, (A.consumers - A.consumers_ios) from (
        SELECT CONCAT(YEAR(%s),',','Q',QUARTER(%s)) s_day,count(DISTINCT PHONEID) consumers,
        count(distinct case when p LIKE '%%ios%%' then PHONEID else null end ) consumers_ios
        FROM `TICKET_ORDERDETAIL` od
        INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        where od.CREATETIME>= %s
        and od.CREATETIME < %s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        AND IFNULL(od.`LINKTYPE`, 0) != 2
        and INTFLAG=1
        ) A
    """
    insert_sql = """
        insert into hbgj_consumers_inter_quarterly (s_day, consumers, consumers_ios, consumers_android,
        createtime, updatetime)
        values
        (%s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        consumers = values(consumers),
        consumers_ios = values(consumers_ios),
        consumers_android = values(consumers_android)
    """
    query_data = DBCli().sourcedb_cli.query_all(inter_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)


def update_hbgj_newconsumers_inter_daily_nation(days=0):
    """国际机票新增消费用户购买过国内机票, hbgj_newconsumers_inter_daily_nation"""
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
        insert into hbgj_newconsumers_inter_daily_nation (s_day, new_consumers, new_consumers_ios, new_consumers_android,
        createtime, updatetime)
        values
        (%s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        new_consumers = values(new_consumers),
        new_consumers_ios = values(new_consumers_ios),
        new_consumers_android = values(new_consumers_android)
    """
    query_data = DBCli().sourcedb_cli.query_all(inter_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)

if __name__ == "__main__":
    # update_hbgj_newconsumers_inter_daily_nation(1)
    # i = 1151
    # while i <= 100000:
    #     query_data = update_hbgj_newconsumers_inter_daily(i)
    #     if query_data:
    #         print query_data[0] + "\t" + str(query_data[1]) + "\t" + str(query_data[2]) + "\t" + str(query_data[3])
    #     else:
    #         break
    #     i += 1
    # update_hbgj_consumers_inter_daily(1)
    # update_hb_newconsumers_daily(1)
    # update_hb_consumers_weekly()
    # update_hb_consumers_monthly()
    # update_hb_consumers_quarterly()


    # import datetime
    # import time
    # start_date = datetime.date(2018, 7, 9)
    # while 1:
    #     start_date, end_date = DateUtil.get_last_week_date(start_date)
    #     update_hbgj_consumers_inter_weekly(start_date, end_date)
    # update_hbgj_consumers_inter_quarterly()

    # import datetime
    # import time
    # start_date = datetime.date(2018, 7, 1)
    # while 1:
    #     start_date, end_date = DateUtil.get_last_month_date(start_date)
    #     update_hbgj_consumers_inter_monthly(start_date, end_date)
    pass
    # import datetime
    # import time
    # start_date = datetime.date(2018, 7, 1)
    # while 1:
    #     start_date, end_date = DateUtil.get_last_quarter_date(start_date)
    #     update_hbgj_consumers_inter_quarterly(start_date, end_date)