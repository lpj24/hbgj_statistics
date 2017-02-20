# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_hb_deplay_insure(days=0):
    fly_order_num_sql = """
        SELECT count(DISTINCT(O.ORDERID)) FROM TICKET_ORDERDETAIL O
        LEFT JOIN TICKET_ORDER T ON O.ORDERID=T.ORDERID
        WHERE O.FLYDATE =  %s
        AND (ORDERSTATUE='5' or ORDERSTATUE='52')
        AND (O.LINKTYPE IS NULL OR O.LINKTYPE='0' or O.LINKTYPE ='1')
        AND O.STATE!='12'
    """

    activity_order_num_sql = """
        SELECT count(*) FROM TICKET_DELAY_CARE WHERE flydate=%s and state='1'
    """

    compensate_order_num_sql = """
        SELECT count(*) FROM TICKET_DELAY_CARE WHERE flydate=%s
        and state='1' and chargetime<>0 and chargenum!=0
    """

    compensate_refund_order_num_sql = """
        SELECT count(*) FROM TICKET_DELAY_CARE c join
        TICKET_ORDERDETAIL d on c.passcardno=d.PASSENGERIDCARDNO and c.orderid=d.ORDERID
        WHERE d.flydate=%s and c.chargecount<>0  and c.chargenum!=0
        and  IFNULL(d.REFUNDID, 0) != 0
    """

    compensate_amount_one_sql = """
        SELECT sum(chargecount) FROM TICKET_DELAY_CARE WHERE
        flydate=%s and state='1'
        and chargetime<>0 and chargenum!=0 and remark!='刷延误宝嫌疑'
    """

    compensate_amount_two_sql = """
        SELECT sum(chargecount) FROM TICKET_DELAY_CARE WHERE flydate=%s
        and state='1' and chargetime<>0
        and multiple=2 and chargenum!=0 and remark!='刷延误宝嫌疑'
    """

    compensate_exception_sql = """
        select count(*) from (
        SELECT count(*) order_num FROM TICKET_DELAY_CARE WHERE flydate=%s
        and state='1' and chargetime<>0 and chargenum!=0 GROUP BY uid HAVING order_num >=2) as A
    """
    query_date = DateUtil.date2str(DateUtil.get_date_before_days(int(days)), '%Y-%m-%d')
    dto = [query_date]

    fly_order_num = DBCli().sourcedb_cli.queryOne(fly_order_num_sql, dto)
    activity_order_num = DBCli().sourcedb_cli.queryOne(activity_order_num_sql, dto)
    compensate_order_num = DBCli().sourcedb_cli.queryOne(compensate_order_num_sql, dto)
    compensate_refund_num = DBCli().sourcedb_cli.queryOne(compensate_refund_order_num_sql, dto)
    compensate_amount_one = DBCli().sourcedb_cli.queryOne(compensate_amount_one_sql, dto)
    compensate_amount_two = DBCli().sourcedb_cli.queryOne(compensate_amount_two_sql, dto)
    compensate_exception_num = DBCli().sourcedb_cli.queryOne(compensate_exception_sql, dto)

    insert_sql = """
        insert into hbgj_delay_treasure_daily (s_day, fly_order_num, activity_order_num, compensate_order_num,
        compensate_refund_order_num, compensate_amount, compensate_execption_num, createtime, updatetime)
        values (%s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        fly_order_num = values(fly_order_num),
        activity_order_num = values(activity_order_num),
        compensate_order_num = values(compensate_order_num),
        compensate_refund_order_num = values(compensate_refund_order_num),
        compensate_amount = values(compensate_amount),
        compensate_execption_num = values(compensate_execption_num)

    """
    compensate_amount_one = int(compensate_amount_one[0]) if compensate_amount_one[0] else 0
    compensate_amount_two = int(compensate_amount_two[0]) if compensate_amount_two[0] else 0
    insert_data = [query_date, fly_order_num[0], activity_order_num[0], compensate_order_num[0],
                   compensate_refund_num[0], compensate_amount_one + compensate_amount_two,
                   compensate_exception_num[0]]

    DBCli().targetdb_cli.insert(insert_sql, insert_data)


def update_compensate_detail(days=0):
    compensate_detail_sql = """
        SELECT chargecount,count(*) order_num FROM TICKET_DELAY_CARE
        WHERE flydate = %s and state='1'
        and chargetime<>0 and chargenum!=0 GROUP BY chargecount
    """

    compensate_detail_insert_sql = """
        insert into delaycare_detail_daily (s_day, delaycare_type, delaycare_count, createtime, updatetime)
        values (%s, %s, %s, now(), now())
    """
    # from collections import OrderedDict
    # compensate_type = OrderedDict()
    # compensate_type[10] = 0
    # compensate_type[30] = 0
    # compensate_type[60] = 0
    # compensate_type[120] = 0
    # compensate_type[200] = 0

    query_date = DateUtil.date2str(DateUtil.get_date_before_days(int(days)), '%Y-%m-%d')
    compensate_detail = DBCli(dict).sourcedb_cli.queryAll(compensate_detail_sql, [query_date])
    for i in compensate_detail:
        DBCli().targetdb_cli.insert(compensate_detail_insert_sql, [query_date, i["chargecount"], i["order_num"]])
    # for i in compensate_detail:
    #     if i["chargecount"] in compensate_type:
    #         compensate_type[i["chargecount"]] = i["order_num"]
    #
    # for k, v in compensate_type.items():
    #     DBCli().targetdb_cli.insert(compensate_detail_insert_sql, [query_date, str(k), v])

if __name__ == "__main__":
    update_hb_deplay_insure(1)
    update_compensate_detail(1)
    # i = 539
    # while i >= 1:
    #     update_compensate_detail(i)
    #     i -= 1