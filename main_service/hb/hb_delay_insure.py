# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_hb_deplay_insure(days=0):
    """更新航班关怀活动(邮件2-6), operation_hbgj_delay_treasure_daily"""
    while int(days) <= 15:
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
            and  IFNULL(d.REFUNDID, 0) != 0 and remark!='刷延误宝嫌疑';

        """
        compensate_amount_one_sql = """
            SELECT sum(chargenum) FROM TICKET_DELAY_CARE WHERE
            flydate=%s and state='1'
            and chargetime<>0 and chargenum!=0 and remark!='刷延误宝嫌疑'
        """

        compensate_amount_two_sql = """
            SELECT sum(chargenum) FROM TICKET_DELAY_CARE WHERE flydate=%s
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

        fly_order_num = DBCli().sourcedb_cli.query_one(fly_order_num_sql, dto)
        activity_order_num = DBCli().sourcedb_cli.query_one(activity_order_num_sql, dto)
        compensate_order_num = DBCli().sourcedb_cli.query_one(compensate_order_num_sql, dto)
        compensate_refund_num = DBCli().sourcedb_cli.query_one(compensate_refund_order_num_sql, dto)
        compensate_amount_one = DBCli().sourcedb_cli.query_one(compensate_amount_one_sql, dto)
        compensate_amount_two = DBCli().sourcedb_cli.query_one(compensate_amount_two_sql, dto)
        compensate_exception_num = DBCli().sourcedb_cli.query_one(compensate_exception_sql, dto)

        insert_sql = """
            insert into operation_hbgj_delay_treasure_daily (s_day, fly_order_num, activity_order_num, compensate_order_num,
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
        days += 1
    pass


def update_compensate_detail(days=0):
    """更新延误宝赔付明细(邮件2-6), operation_delaycare_detail_daily"""
    compensate_detail_sql = """
        SELECT DATE_FORMAT(flydate, '%%Y-%%m-%%d') s_day, chargenum,count(*) order_num FROM TICKET_DELAY_CARE
        WHERE flydate >= %s
        and flydate < %s and state='1'
        and chargetime<>0 and chargenum!=0 GROUP BY chargenum, s_day
        order by s_day
    """

    compensate_detail_insert_sql = """
        insert into operation_delaycare_detail_daily (s_day, delaycare_type, delaycare_count, createtime, updatetime)
        values (%s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        delaycare_type = values(delaycare_type),
        delaycare_count = values(delaycare_count)
    """

    query_start = DateUtil.date2str(DateUtil.get_date_before_days(int(days) * 15), '%Y-%m-%d')
    query_end = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)), '%Y-%m-%d')
    compensate_detail = DBCli().sourcedb_cli.query_all(compensate_detail_sql, [query_start, query_end])
    DBCli().targetdb_cli.batch_insert(compensate_detail_insert_sql, compensate_detail)

    pass

if __name__ == "__main__":
    # update_hb_deplay_insure(2)
    update_compensate_detail(1)
    update_hb_deplay_insure(1)