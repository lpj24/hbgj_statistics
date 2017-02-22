# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_hb_insure_daily():
    start_date, end_date = DateUtil.get_last_week_date()
    start_date, end_date = DateUtil.date2str(start_date, '%Y-%m-%d'), DateUtil.date2str(start_date, '%Y-%m-%d')

    boat_sql = """
        SELECT left(CREATETIME,10),count(*) FROM
        `INSURE_ORDERDETAIL` where createtime
        BETWEEN %s and %s and insurecode in
        ('PA_A','A','ABE','ABE30','ABE_HZ','PA_D','ABE_YG','A_QUNAYSFYJHS')
        GROUP BY left(CREATETIME,10)
    """

    refund_sql = """
        SELECT left(CREATETIME,10),count(*) FROM
        `INSURE_ORDERDETAIL` where createtime BETWEEN
        %s and %s and
        insurecode in ('PA_G','HT_G','PA35_G')  GROUP BY left(CREATETIME,10)
    """

    delay_sql = """
        SELECT left(CREATETIME,10),count(*) FROM
        `INSURE_ORDERDETAIL` where createtime BETWEEN
        %s and %s and insurecode in
        ('PICC_D20','PICC_D25','PICC_D30','PA_D')  GROUP BY left(CREATETIME,10)
    """

    platform_sql = """
        SELECT left(od.CREATETIME,10),count(*),sum(od.OUTPAYPRICE)
        FROM `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER` o
        on od.ORDERID=o.ORDERID where od.CREATETIME BETWEEN
        %s and %s and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
        IFNULL(od.`LINKTYPE`, 0) != 2 GROUP BY left(od.CREATETIME,10)
    """

