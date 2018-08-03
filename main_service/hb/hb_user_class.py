# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_hbgj_user_class(days=1):
    consumers_order_ticket_sql = """
        SELECT DATE_FORMAT(TICKET_ORDER.CREATETIME,'%Y-%m-%d') s_day,
        TICKET_ORDERDETAIL.ORDERID,
        TICKET_ORDER.PHONEID,
        TICKET_ORDERDETAIL.OUTPAYPRICE
        FROM TICKET_ORDERDETAIL
        join TICKET_ORDER
        ON TICKET_ORDER.ORDERID = TICKET_ORDERDETAIL.ORDERID
        where TICKET_ORDER.ORDERSTATUE not in (2,12,21,51,75)
        and  TICKET_ORDER.CREATETIME >= '2018-02-01'
        and  TICKET_ORDER.CREATETIME < '2018-02-02';
    """

    platform_insure_sql = """
        SELECT left(od.CREATETIME,10),od.OUTPAYPRICE, o.PHONEID
        FROM `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER` o
        on od.ORDERID=o.ORDERID where od.CREATETIME BETWEEN
        '2018-02-01' and '2018-02-02' and 
        o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
        IFNULL(od.`LINKTYPE`, 0) != 2;
    """

    boat_sql = """
        SELECT tod.PHONEID,INSURE_ORDERDETAIL.OUTORDERID,left(INSURE_ORDERDETAIL.CREATETIME,10) FROM
        INSURE_ORDERDETAIL
        INNER JOIN TICKET_ORDER tod on INSURE_ORDERDETAIL.OUTORDERID=tod.ORDERID
        where INSURE_ORDERDETAIL.createtime
        BETWEEN '2018-02-01' and '2018-02-02' and
        INSURE_ORDERDETAIL.insurecode in 
        (select DISTINCT id from INSURE_DATA where bigtype=2)
    """

    refund_sql = """
        SELECT tod.PHONEID,INSURE_ORDERDETAIL.OUTORDERID,left(INSURE_ORDERDETAIL.CREATETIME,10) FROM
        INSURE_ORDERDETAIL
        INNER JOIN TICKET_ORDER tod on INSURE_ORDERDETAIL.OUTORDERID=tod.ORDERID
        where INSURE_ORDERDETAIL.createtime
        BETWEEN '2018-02-01' and '2018-02-02' and
        INSURE_ORDERDETAIL.insurecode in 
        (select DISTINCT id from INSURE_DATA where bigtype=3);
    """

    delay_sql = """
        SELECT tod.PHONEID,INSURE_ORDERDETAIL.OUTORDERID,left(INSURE_ORDERDETAIL.CREATETIME,10) FROM
        INSURE_ORDERDETAIL
        INNER JOIN TICKET_ORDER tod on INSURE_ORDERDETAIL.OUTORDERID=tod.ORDERID
        where INSURE_ORDERDETAIL.createtime
        BETWEEN '2018-02-01' and '2018-02-02' and
        INSURE_ORDERDETAIL.insurecode in 
        (select DISTINCT id from INSURE_DATA where bigtype=1);
    """