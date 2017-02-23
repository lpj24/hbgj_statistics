# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_hb_insure_daily(start_date, end_date):
    # start_date, end_date = DateUtil.get_last_week_date()
    start_date, end_date = DateUtil.date2str(start_date, '%Y-%m-%d'), DateUtil.date2str(end_date, '%Y-%m-%d')

    platform_sql = """
        SELECT left(od.CREATETIME,10),count(*),sum(od.OUTPAYPRICE)
        FROM `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER` o
        on od.ORDERID=o.ORDERID where od.CREATETIME BETWEEN
        %s and %s and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
        IFNULL(od.`LINKTYPE`, 0) != 2 GROUP BY left(od.CREATETIME,10)
    """

    update_platform_sql = """
            insert into operation_hbgj_insure (s_day, platform_ticket_num, platform_amount, createtime, updatetime)
            values (%s, %s ,%s, now(), now())
    """

    boat_sql = """
        SELECT count(*),left(CREATETIME,10) FROM
        `INSURE_ORDERDETAIL` where createtime
        BETWEEN %s and %s and insurecode in
        ('PA_A','A','ABE','ABE30','ABE_HZ','PA_D','ABE_YG','A_QUNAYSFYJHS')
        GROUP BY left(CREATETIME,10)
    """

    update_boat_sql = """
        update operation_hbgj_insure set boat_order_num=%s where s_day = %s
    """

    refund_sql = """
        SELECT count(*), left(CREATETIME,10) FROM
        `INSURE_ORDERDETAIL` where createtime BETWEEN
        %s and %s and
        insurecode in ('PA_G','HT_G','PA35_G')  GROUP BY left(CREATETIME,10)
    """

    update_refund_ticket_sql = """
        update operation_hbgj_insure set refund_ticket_order_num = %s where s_day = %s
    """

    delay_sql = """
        SELECT count(*), left(CREATETIME,10) FROM
        `INSURE_ORDERDETAIL` where createtime BETWEEN
        %s and %s and insurecode in
        ('PICC_D20','PICC_D25','PICC_D30','PA_D')  GROUP BY left(CREATETIME,10)
    """

    update_delay_sql = """
        update operation_hbgj_insure set delay_order_num=%s where s_day=%s
    """
    dto = [start_date, end_date]
    platform_data = DBCli().sourcedb_cli.queryAll(platform_sql, dto)
    DBCli().targetdb_cli.batchInsert(update_platform_sql, platform_data)

    boat_data = DBCli().sourcedb_cli.queryAll(boat_sql, dto)
    DBCli().targetdb_cli.batchInsert(update_boat_sql, boat_data)

    refund_data = DBCli().sourcedb_cli.queryAll(refund_sql, dto)
    DBCli().targetdb_cli.batchInsert(update_refund_ticket_sql, refund_data)

    delay_data = DBCli().sourcedb_cli.queryAll(delay_sql, dto)
    DBCli().targetdb_cli.batchInsert(update_delay_sql, delay_data)


def update_insure_class_daily(days=0):
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1-days)
    hbgj_insure_sql = """
        SELECT
        left(i.createtime,10), "hbgj",
        i.insurecode,
        count(case when o.intflag=1 then 1 end) inter_count,
        sum(case when o.intflag=1 then i.PRICE else 0 end) inter_price,
        count(case when o.intflag=0 then 1 end) inland_count,
        sum(case when o.intflag=0 then i.PRICE else 0 end) inland_price
        FROM skyhotel.`INSURE_ORDERDETAIL` i
        join skyhotel.`TICKET_ORDER` o on i.outorderid=o.orderid
        where i.createtime BETWEEN %s and %s
        and i.insurecode in
        ('PA_A','A','ABE','ABE30','ABE_HZ','PA_D','ABE_YG','A_QUNAYSFYJHS',
        'PA_G','HT_G','PA35_G','PICC_D20','PICC_D25','PICC_D30') and o.p like '%%hbgj%%'
        GROUP BY left(i.createtime,10), i.insurecode
    """

    gtgj_insure_sql = """
        SELECT
        left(i.createtime,10), "gtgj",
        i.insurecode,
        count(case when o.intflag=1 then 1 end) inter_count,
        sum(case when o.intflag=1 then i.PRICE else 0 end) inter_price,
        count(case when o.intflag=0 then 1 end) inland_count,
        sum(case when o.intflag=0 then i.PRICE else 0 end) inland_price
        FROM skyhotel.`INSURE_ORDERDETAIL` i
        join skyhotel.`TICKET_ORDER` o on i.outorderid=o.orderid
        where i.createtime BETWEEN %s and %s
        and i.insurecode in
        ('PA_A','A','ABE','ABE30','ABE_HZ','PA_D','ABE_YG','A_QUNAYSFYJHS',
        'PA_G','HT_G','PA35_G','PICC_D20','PICC_D25','PICC_D30') and o.p like '%%gtgj%%'
        GROUP BY left(i.createtime,10), i.insurecode
    """

    other_insure_sql = """
        SELECT
        left(i.createtime,10), "other_platform",
        i.insurecode,
        count(case when o.intflag=1 then 1 end) inter_count,
        sum(case when o.intflag=1 then i.PRICE else 0 end) inter_price,
        count(case when o.intflag=0 then 1 end) inland_count,
        sum(case when o.intflag=0 then i.PRICE else 0 end) inland_price
        FROM skyhotel.`INSURE_ORDERDETAIL` i
        join skyhotel.`TICKET_ORDER` o on i.outorderid=o.orderid
        where i.createtime BETWEEN %s and %s
        and i.insurecode in
        ('PA_A','A','ABE','ABE30','ABE_HZ','PA_D','ABE_YG','A_QUNAYSFYJHS',
        'PA_G','HT_G','PA35_G','PICC_D20','PICC_D25','PICC_D30')
        and o.p not like '%%gtgj%%' and o.p not like '%%hbgj%%'
        GROUP BY left(i.createtime,10), i.insurecode
    """

    insert_sql = """
        insert into operation_hbgj_insure_platform_daily (s_day, platform, insure_code, inter_insure_num,
        inter_insure_amount, inland_insure_num, inland_insure_amount, createtime, updatetime)
        values (%s, %s, %s, %s, %s, %s, %s, now(), now())
    """

    dto = [start_date, end_date]
    hbgj_data = DBCli().sourcedb_cli.queryAll(hbgj_insure_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_sql, hbgj_data)

    gtgj_data = DBCli().sourcedb_cli.queryAll(gtgj_insure_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_sql, gtgj_data)

    other_data = DBCli().sourcedb_cli.queryAll(other_insure_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_sql, other_data)


if __name__ == "__main__":
    # import datetime
    # min_date = datetime.date(2013, 4, 26)
    # start_date, end_date = DateUtil.get_last_week_date()
    # while start_date >= min_date:
    #     start_date, end_date = DateUtil.get_last_week_date(start_date)
    #     update_hb_insure_daily(start_date, end_date)
    #     print start_date, end_date
    # update_hb_insure_daily()
    update_insure_class_daily(2)


