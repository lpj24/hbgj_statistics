# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_hbgj_income_issue_refund_daily(days=0):
    """机票收入按照客户端类型/出(退)票, hbgj_income_issue_refund_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    hb_gt_sql = """
        SELECT %s,
        count(case when p like '%%hbgj%%' and i.INCOMEITEM=0 then i.TICKETNO end) hbgj_issue_ticket_num,
        sum(case when p like '%%hbgj%%' and i.INCOMEITEM=0 then i.income end) hbgj_issue_amount,
        count(case when p like '%%gtgj%%' and i.INCOMEITEM=0 then i.TICKETNO end) gtgj_issue_ticket_num,
        sum(case when p like '%%gtgj%%' and i.INCOMEITEM=0 then i.income end) gtgj_issue_amount,
        count(case when p like '%%hbgj%%' and i.INCOMEITEM=1 then i.TICKETNO end) hbgj_refund_ticket_num,
        sum(case when p like '%%hbgj%%' and i.INCOMEITEM=1 then i.income end) hbgj_refund_amount,
        count(case when p like '%%gtgj%%' and i.INCOMEITEM=1 then i.TICKETNO end) gtgj_refund_ticket_num,
        sum(case when p like '%%gtgj%%' and i.INCOMEITEM=1 then i.income end) gtgj_refund_amount
        FROM `TICKET_ORDER_INCOME` i
        join `TICKET_ORDER` o on i.orderid=o.orderid
        where i.type=0
        and i.INCOMEDATE=%s
    """
    insert_sql = """
        insert into hbgj_income_issue_refund_daily (s_day, hbgj_issue_ticket_num, hbgj_issue_amount,
        gtgj_issue_ticket_num, gtgj_issue_amount, hbgj_refund_ticket_num, hbgj_refund_amount,
        gtgj_refund_ticket_num, gtgj_refund_amount, createtime, updatetime)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now() ,
        s_day = VALUES(s_day),
        hbgj_issue_ticket_num = VALUES(hbgj_issue_ticket_num),
        hbgj_issue_amount = VALUES(hbgj_issue_amount),
        gtgj_issue_ticket_num = VALUES(gtgj_issue_ticket_num),
        gtgj_issue_amount = VALUES(gtgj_issue_amount),
        hbgj_refund_ticket_num = VALUES(hbgj_refund_ticket_num),
        hbgj_refund_amount = VALUES(hbgj_refund_amount),
        gtgj_refund_ticket_num = VALUES(gtgj_refund_ticket_num),
        gtgj_refund_amount = VALUES(gtgj_refund_amount)
    """
    hb_gt_income_data = DBCli().sourcedb_cli.queryOne(hb_gt_sql, [start_date] * 2)
    DBCli().targetdb_cli.insert(insert_sql, hb_gt_income_data)
    return __file__


def update_hbgj_cost_type_daily(days=0):
    """机票成本 按照客户端类型/补贴方式/出(退)票分类, hbgj_cost_type_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    hbgj_cost_sql = """
        SELECT c.PNRSOURCE,count(c.odid),sum(c.amount)
        FROM `TICKET_ORDER_COST` c
        join `TICKET_ORDER` o on c.orderid=o.orderid
        where c.AMOUNTTYPE=%s  and c.type=%s
        and COSTTYPE=%s
        and c.COSTDATE=%s
        and p like '%%{}%%'
    """

    insert_sql = """
        insert into hbgj_cost_type_daily
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
    """
    hbgj_bonus_user_issue_dto = [0, 0, 0, start_date, 'hbgj']
    gtgj_bonus_user_issue_dto = [0, 0, 0, start_date, 'gtgj']
    hbgj_bonus_channel_issue_dto = [1, 0, 0, start_date, 'hbgj']
    gtgj_bonus_channel_issue_dto = [1, 0, 0, start_date, 'gtgj']

    hbgj_bonus_return_issue_dto = [2, 0, 0, start_date, 'hbgj']
    gtgj_bonus_return_issue_dto = [2, 0, 0, start_date, 'gtgj']

    #退票
    hbgj_bonus_user_refund_dto = [0, 0, 1, start_date, 'hbgj']
    gtgj_bonus_user_refund_dto = [0, 0, 1, start_date, 'gtgj']
    hbgj_bonus_channel_refund_dto = [1, 0, 1, start_date, 'hbgj']
    gtgj_bonus_channel_refund_dto = [1, 0, 1, start_date, 'gtgj']

    hbgj_bonus_return_refund_dto = [2, 0, 1, start_date, 'hbgj']
    gtgj_bonus_return_refund_dto = [2, 0, 1, start_date, 'gtgj']
    query_dto = [hbgj_bonus_user_issue_dto, gtgj_bonus_user_issue_dto, hbgj_bonus_channel_issue_dto,
                 gtgj_bonus_channel_issue_dto, hbgj_bonus_return_issue_dto, gtgj_bonus_return_issue_dto,
                 hbgj_bonus_user_refund_dto, gtgj_bonus_user_refund_dto, hbgj_bonus_channel_refund_dto,
                 gtgj_bonus_channel_refund_dto, hbgj_bonus_return_refund_dto, gtgj_bonus_return_refund_dto]

    insert_data = [start_date, ]
    for dto in query_dto:
        hbgj_new_cost_sql = hbgj_cost_sql.format(dto.pop())
        query_data = DBCli().sourcedb_cli.queryOne(hbgj_new_cost_sql, dto)
        insert_data.extend(list(query_data))

    DBCli().targetdb_cli.insert(insert_sql, insert_data)
    return __file__


def update_hbgj_no_transfer_order_income_cost_daily(days=0):
    """更新非转单收入成本, hbgj_no_transfer_order_income_cost"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1-days), '%Y-%m-%d')
    transfer_income_sql = """
        SELECT
        sum(case when i.INCOMEITEM=0 then i.INCOME end) issue_income,
        sum(case when i.INCOMEITEM=1 then i.INCOME end) refund_income
        FROM `TICKET_ORDER_INCOME` i join
        skyhotel.`TICKET_ORDERDETAIL` od on i.TICKETNO=od.ODID
        left join skyhotel.`TICKET_ORDER` o on i.ORDERID=o.ORDERID
        where i.type=0 and i.PNRSOURCE='hlth'
        and i.INCOMEDATE >=%s and i.INCOMEDATE < %s
        and od.LINKTYPE is NULL and o.mode=0  and od.LINKDETAILID=0
        GROUP BY i.INCOMEDATE
    """
    transfer_cost_sql = """
        SELECT
        sum(case when c.COSTTYPE=0 then c.amount end) issue_cost,
        sum(case when c.COSTTYPE=1 then c.amount end) refund_cost
        FROM `TICKET_ORDER_COST` c join skyhotel.`TICKET_ORDERDETAIL`
        od on c.ODID=od.ODID left join skyhotel.`TICKET_ORDER` o on c.ORDERID=o.ORDERID
        where c.PNRSOURCE='hlth' and c.type=0
        and c.COSTDATE >= %s and  c.COSTDATE < %s
        and c.AMOUNTTYPE!=2   and od.LINKTYPE is NULL and o.mode=0
        and od.LINKDETAILID=0 GROUP BY  COSTDATE;
    """

    insert_sql = """
        insert into hbgj_no_transfer_order_income_cost_daily (s_day, no_transfer_issue_income,
        no_transfer_refund_income, no_transfer_issue_cost, no_transfer_refund_cost,
        createtime, updatetime) values (%s, %s, %s, %s, %s, now(), now())
    """
    dto = [start_date, end_date]
    income_data = DBCli().sourcedb_cli.queryOne(transfer_income_sql, dto)
    cost_data = DBCli().sourcedb_cli.queryOne(transfer_cost_sql, dto)
    insert_data = [start_date] + list(income_data) + list(cost_data)
    DBCli().targetdb_cli.insert(insert_sql, insert_data)
    return __file__


def update_hbgj_transfer_order_income_cost_daily(days=0):
    """转单收入与成本, hbgj_transfer_order_income_cost_daily"""


if __name__ == "__main__":
    # update_hbgj_income_issue_refund_daily(1)
    # update_hbgj_cost_type_daily(10)
    update_hbgj_no_transfer_order_income_cost_daily(1)