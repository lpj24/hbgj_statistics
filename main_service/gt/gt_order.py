# -*- coding: utf-8 -*-
from sql.gt_sqlHandlers import gt_order_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_gt_order_daily(days=0):
    """高铁订单(日), gtgj_order_daily"""
    dto = []
    if days > 0:
        today = DateUtil.date2str(DateUtil.get_date_before_days(3))
        tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(0))
        for i in xrange(0, 4):
            dto.append(today)
            dto.append(tomorrow)
        query_data = DBCli().gt_cli.query_all(gt_order_sql["gtgj_order_daily_his"], dto)
    else:
        today = DateUtil.date2str(DateUtil.get_date_before_days(days))
        tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(1))
        for i in xrange(0, 6):
            dto.append(today)
            dto.append(tomorrow)
        query_data = DBCli().gt_cli.query_all(gt_order_sql["gtgj_order_daily"], dto)

    DBCli().targetdb_cli.batch_insert(gt_order_sql["update_gtgj_order_daily"], query_data)


def update_gt_order_hourly(days=0):
    today = DateUtil.date2str(DateUtil.get_date_before_days(days))
    tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(1-int(days)))
    dto = [today, tomorrow]
    query_data = DBCli().gt_cli.query_all(gt_order_sql["gtgj_order_hourly"], dto)
    for hour_data in query_data:
        tmp_dto = [hour_data[0], hour_data[1]]
        q_hour_data = DBCli().targetdb_cli.query_one(gt_order_sql["query_gtgj_order_by_hour"], tmp_dto)
        if q_hour_data:
            DBCli().targetdb_cli.insert(gt_order_sql["update_gtgj_order_hourly"], hour_data[2:]+hour_data[0:2])
        else:
            DBCli().targetdb_cli.insert(gt_order_sql["insert_gtgj_order_hourly"], hour_data)


def update_hb_gt_book_daily(days=0):
    """航班高铁预定时间段, hb_gt_book_diff_days_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 1), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - days), '%Y-%m-%d')
    dto = [start_date, end_date]
    hb_sql = """
        SELECT DATE_FORMAT(od.CREATETIME, '%%Y-%%m-%%d') s_day, 
        TIMESTAMPDIFF(DAY, DATE_FORMAT(od.CREATETIME,'%%Y-%%m-%%d'), FLYDATE) diff_day, 
        COUNT(*) ticket_num
        FROM `TICKET_ORDERDETAIL` od  
        INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID  
        where od.CREATETIME>=%s
        and od.CREATETIME<%s  
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)   
        and o.INTFLAG=0 AND IFNULL(od.`LINKTYPE`, 0) != 2   
        and od.REFUNDID=0  
        and o.`MODE`=0   
        and p like '%%hbgj%%'
        GROUP BY diff_day, s_day;
    """

    insert_sql = """
        insert into hb_gt_book_diff_days_daily (s_day, channel, book_day0, book_day1, book_day2, book_day3_7, 
        book_day7_14, book_day14_30, book_day30, createtime, updatetime) values
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        channel = values(channel),
        book_day0 = values(book_day0),
        book_day1 = values(book_day1),
        book_day2 = values(book_day2),
        book_day3_7 = values(book_day3_7),
        book_day7_14 = values(book_day7_14),
        book_day14_30 = values(book_day14_30),
        book_day30 = values(book_day30)
    """
    from collections import defaultdict
    insert_data = defaultdict(int)
    hb_data = DBCli().sourcedb_cli.query_all(hb_sql, dto)

    def case_day(data):
        for d in data:
            s_day, days, ticket_num = d
            if days == 0:
                insert_key = "0"
            elif days == 1:
                insert_key = "1"
            elif days == 2:
                insert_key = "2"
            elif 3 <= days < 7:
                insert_key = "3_7"
            elif 7 <= days < 14:
                insert_key = "7_14"
            elif 14 <= days < 30:
                insert_key = "14_30"
            elif days >= 30:
                insert_key = "30"

            insert_data[insert_key] += ticket_num

    case_day(hb_data)
    insert_hb_data = [start_date, 'hbgj', insert_data["0"], insert_data["1"], insert_data["2"],
                      insert_data["3_7"], insert_data["7_14"], insert_data["14_30"],
                      insert_data["30"]]
    DBCli().targetdb_cli.batch_insert(insert_sql, [insert_hb_data])
    insert_data.clear()

    gt_sql = """
        SELECT DATE_FORMAT(od.CREATETIME,'%%Y-%%m-%%d') s_day,
        TIMESTAMPDIFF(DAY, DATE_FORMAT(od.CREATETIME,'%%Y-%%m-%%d'), FLYDATE) diff_day,
        COUNT(*) ticket_num
        FROM `TICKET_ORDERDETAIL` od
        INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        where od.CREATETIME>=%s
        and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        and o.INTFLAG=0 AND IFNULL(od.`LINKTYPE`, 0) != 2
        and od.REFUNDID=0
        and o.`MODE`=0
        and p like '%%gtgj%%'
        GROUP BY diff_day, s_day;
    """

    gt_data = DBCli().sourcedb_cli.query_all(gt_sql, dto)
    case_day(gt_data)
    insert_gt_data = [start_date, 'gtgj', insert_data["0"], insert_data["1"], insert_data["2"],
                      insert_data["3_7"], insert_data["7_14"], insert_data["14_30"],
                      insert_data["30"]]
    DBCli().targetdb_cli.batch_insert(insert_sql, [insert_gt_data])


if __name__ == "__main__":
    i = 1
    while i <= 84:
        update_hb_gt_book_daily(i)
        i += 1
    # update_gt_order_hourly()
