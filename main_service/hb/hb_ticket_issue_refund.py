# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_hbgj_income_issue_refund_daily(days=0):
    """机票收入按照客户端类型/出(退)票, profit_hb_income_type_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 5), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1-days), '%Y-%m-%d')
    hb_gt_sql = """
        SELECT DATE_FORMAT(i.INCOMEDATE, '%%Y-%%m-%%d') s_day,
        count(case when p like '%%hbgj%%' and i.INCOMEITEM=0 then 1 end) hbgj_issue_ticket_num,
        sum(case when p like '%%hbgj%%' and i.INCOMEITEM=0 then i.income end) hbgj_issue_income,
        count(case when p like '%%gtgj%%' and i.INCOMEITEM=0 then 1 end) gtgj_issue_ticket_num,
        sum(case when p like '%%gtgj%%' and i.INCOMEITEM=0 then i.income end) gtgj_issue_income,
        count(case when p like '%%hbgj%%' and i.INCOMEITEM=1 then 1 end) hbgj_refund_ticket_num,
        sum(case when p like '%%hbgj%%' and i.INCOMEITEM=1 then i.income end) hbgj_refund_income,
        count(case when p like '%%gtgj%%' and i.INCOMEITEM=1 then 1 end) gtgj_refund_ticket_num,
        sum(case when p like '%%gtgj%%' and i.INCOMEITEM=1 then i.income end) gtgj_refund_income,
        count(case when p not like '%%hbgj%%' and p not like '%%gtgj%%' and i.INCOMEITEM=0 then 1 end) else_issue_ticket_num,
        sum(case when p not like '%%hbgj%%' and p not like '%%gtgj%%' and i.INCOMEITEM=0 then i.income end) else_issue_income,
        count(case when p not like '%%hbgj%%' and p not like '%%gtgj%%' and i.INCOMEITEM=1 then 1 end) else_refund_ticket_num,
        sum(case when p not like '%%hbgj%%' and p not like '%%gtgj%%' and i.INCOMEITEM=1 then i.income else 0 end) else_refund_income
        FROM `TICKET_ORDER_INCOME` i
        join `TICKET_ORDER` o on i.orderid=o.orderid
        where i.type=0
        and i.INCOMEDATE>=%s
        and i.INCOMEDATE<%s
        GROUP BY s_day
    """
    insert_sql = """
        insert into profit_hb_income_type_daily (s_day, hbgj_issue_ticket_num, hbgj_issue_income,
        gtgj_issue_ticket_num, gtgj_issue_income, hbgj_refund_ticket_num, hbgj_refund_income,
        gtgj_refund_ticket_num, gtgj_refund_income, else_issue_ticket_num,
        else_issue_income,
        else_refund_ticket_num,
        else_refund_income,
        createtime, updatetime)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now() ,
        s_day = VALUES(s_day),
        hbgj_issue_ticket_num = VALUES(hbgj_issue_ticket_num),
        hbgj_issue_income = VALUES(hbgj_issue_income),
        gtgj_issue_ticket_num = VALUES(gtgj_issue_ticket_num),
        gtgj_issue_income = VALUES(gtgj_issue_income),
        hbgj_refund_ticket_num = VALUES(hbgj_refund_ticket_num),
        hbgj_refund_income = VALUES(hbgj_refund_income),
        gtgj_refund_ticket_num = VALUES(gtgj_refund_ticket_num),
        gtgj_refund_income = VALUES(gtgj_refund_income),
        else_issue_ticket_num = VALUES(else_issue_ticket_num),
        else_issue_income = VALUES(else_issue_income),
        else_refund_ticket_num = VALUES(else_refund_ticket_num),
        else_refund_income = VALUES(else_refund_income)
    """
    hb_gt_income_data = DBCli().sourcedb_cli.queryAll(hb_gt_sql, [start_date, end_date])
    DBCli().targetdb_cli.batchInsert(insert_sql, hb_gt_income_data)
    return __file__


def update_hbgj_cost_type_daily(days=0):
    """机票成本 按照客户端类型/补贴方式/出(退)票分类, profit_hb_cost_type_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    hbgj_cost_sql = """
        SELECT sum(c.amount), count(c.odid) ticket_num, c.COSTDATE, %s, %s, c.PNRSOURCE,
        PNRSOURCE_CONFIG.`NAME`
        FROM `TICKET_ORDER_COST` c
        join `TICKET_ORDER` o on c.orderid=o.orderid
        left join PNRSOURCE_CONFIG on c.PNRSOURCE=PNRSOURCE_CONFIG.PNRSOURCE
        where c.AMOUNTTYPE in %s and c.type=0
        and COSTTYPE=%s
        and c.COSTDATE=%s
        and p like '%%{}%%'
        GROUP BY c.COSTDATE, c.PNRSOURCE, PNRSOURCE_CONFIG.`NAME`;
    """

    else_cost_sql = """
        SELECT sum(c.amount), count(c.odid) ticket_num, c.COSTDATE, %s, %s, c.PNRSOURCE,
        PNRSOURCE_CONFIG.`NAME`
        FROM `TICKET_ORDER_COST` c
        join `TICKET_ORDER` o on c.orderid=o.orderid
        left join PNRSOURCE_CONFIG on c.PNRSOURCE=PNRSOURCE_CONFIG.PNRSOURCE
        where c.AMOUNTTYPE in %s and c.type=0
        and COSTTYPE=%s
        and c.COSTDATE=%s
        and p not like '%%hbgj%%' and p not like '%%gtgj%%'
        GROUP BY c.COSTDATE, c.PNRSOURCE, PNRSOURCE_CONFIG.`NAME`;
    """
    insert_sql = """
        insert into profit_hb_cost_type_daily
        (cost, ticket_num, s_day, platform, amounttype, pnrsource, pnrsource_name, createtime,
        updatetime) values (%s, %s, %s, %s, %s, %s, %s, now(), now())
    """

    update_sql = """
        update profit_hb_cost_type_daily set refund_cost=%s, refund_ticket_num=%s where
        s_day=%s and platform=%s and amounttype=%s and pnrsource=%s and pnrsource_name=%s
    """

    insert_refund_cost_sql = """
        insert into profit_hb_cost_type_daily (refund_cost, refund_ticket_num, s_day, platform,
        amounttype, pnrsource, pnrsource_name, cost, ticket_num, createtime, updatetime)
        values (%s, %s, %s, %s, %s, %s, %s, 0, 0, now(), now())
    """

    check_income_sql = """
        select count(*) from profit_hb_cost_type_daily where s_day=%s and platform=%s
        and amounttype=%s and pnrsource=%s and pnrsource_name=%s
    """
    hbgj_bonus_user_issue_dto = ['hbgj', 0, (0, ), 0, start_date]
    gtgj_bonus_user_issue_dto = ['gtgj', 0, (0, ), 0, start_date]
    else_bonus_user_issue_dto = ['else', 0, (0, ), 0, start_date]

    hbgj_bonus_channel_issue_dto = ['hbgj', 1, (1, ), 0, start_date]
    gtgj_bonus_channel_issue_dto = ['gtgj', 1, (1, ), 0, start_date]
    else_bonus_channel_issue_dto = ['else', 1, (1, ), 0, start_date]

    hbgj_bonus_return_issue_dto = ['hbgj', 2, (2, 3), 0, start_date]
    gtgj_bonus_return_issue_dto = ['gtgj', 2, (2, 3), 0, start_date]
    else_bonus_return_issue_dto = ['else', 2, (2, 3), 0, start_date]

    #退票
    hbgj_bonus_user_refund_dto = ['hbgj', 0, (0, ), 1, start_date]
    gtgj_bonus_user_refund_dto = ['gtgj', 0, (0, ), 1, start_date]
    else_bonus_user_refund_dto = ['else', 0, (0, ), 1, start_date]

    hbgj_bonus_channel_refund_dto = ['hbgj', 1, (1, ), 1, start_date]
    gtgj_bonus_channel_refund_dto = ['gtgj', 1, (1, ), 1, start_date]
    else_bonus_channel_refund_dto = ['else', 1, (1, ), 1, start_date]

    hbgj_bonus_return_refund_dto = ['hbgj', 2, (2, 3), 1, start_date]
    gtgj_bonus_return_refund_dto = ['gtgj', 2, (2, 3), 1, start_date]
    else_bonus_return_refund_dto = ['else', 2, (2, 3), 1, start_date]

    query_issue_dto = [hbgj_bonus_user_issue_dto, gtgj_bonus_user_issue_dto, else_bonus_user_issue_dto,
                       hbgj_bonus_channel_issue_dto, gtgj_bonus_channel_issue_dto, else_bonus_channel_issue_dto,
                       hbgj_bonus_return_issue_dto, gtgj_bonus_return_issue_dto, else_bonus_return_issue_dto]

    query_refund_dto = [hbgj_bonus_user_refund_dto, gtgj_bonus_user_refund_dto, else_bonus_user_refund_dto,
                        hbgj_bonus_channel_refund_dto, gtgj_bonus_channel_refund_dto, else_bonus_channel_refund_dto,
                        hbgj_bonus_return_refund_dto, gtgj_bonus_return_refund_dto, else_bonus_return_refund_dto]

    for index, dto in enumerate(query_issue_dto):
        if dto[0] != "else":
            issue_cost_sql = hbgj_cost_sql.format(dto[0])
            refund_cost_sql = hbgj_cost_sql.format((query_refund_dto[index])[0])
        else:
            issue_cost_sql = else_cost_sql
            refund_cost_sql = else_cost_sql
        issue_cost = DBCli().sourcedb_cli.queryAll(issue_cost_sql, dto)
        DBCli().targetdb_cli.batchInsert(insert_sql, issue_cost)

        refund_cost = DBCli().sourcedb_cli.queryAll(refund_cost_sql, query_refund_dto[index])
        for refund_data in refund_cost:
            check_dto = refund_data[2:]
            income_count = DBCli().targetdb_cli.queryOne(check_income_sql, check_dto)
            if income_count[0] == 0:
                DBCli().targetdb_cli.insert(insert_refund_cost_sql, refund_data)
            else:
                DBCli().targetdb_cli.insert(update_sql, refund_data)
        # DBCli().targetdb_cli.batchInsert(update_sql, refund_cost)
    return __file__


def update_profit_hb_self_no_transfer_daily(days=0):
    """更新自营非转单收入成本, profit_hb_self_no_transfer_daily"""
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
        insert into profit_hb_self_no_transfer_daily (s_day, no_transfer_issue_income,
        no_transfer_refund_income, no_transfer_issue_cost, no_transfer_refund_cost,
        createtime, updatetime) values (%s, %s, %s, %s, %s, now(), now())
    """
    dto = [start_date, end_date]
    income_data = DBCli().sourcedb_cli.queryOne(transfer_income_sql, dto)
    cost_data = DBCli().sourcedb_cli.queryOne(transfer_cost_sql, dto)
    insert_data = [start_date] + list(income_data) + list(cost_data)
    DBCli().targetdb_cli.insert(insert_sql, insert_data)
    return __file__


def update_profit_hb_self_transfer_daily(days=0):
    """自营转单收入与成本, profit_hb_self_transfer_daily"""

    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1-days), '%Y-%m-%d')

    transfer_income_sql = """
        SELECT
        sum(case when i.INCOMEITEM=0 and od.LINKTYPE is NULL then i.INCOME else 0 end) one_issue_income1,
        sum(case when i.INCOMEITEM=0 and od.LINKTYPE is NULL then od.REALPRICE +  od.AIRPORTFEE else 0 end) * 0.005 one_issue_income2,
        sum(case when i.INCOMEITEM=1 and od.LINKTYPE is NULL then i.INCOME else 0 end) one_refund_income,
        sum(case when i.INCOMEITEM=0 and od.LINKTYPE=1 then i.INCOME else 0 end) two_issue_income,
        sum(case when i.INCOMEITEM=1 and od.LINKTYPE=1 then i.INCOME else 0 end) two_refund_income
        FROM `TICKET_ORDER_INCOME` i
        join skyhotel.`TICKET_ORDERDETAIL` od
        on i.TICKETNO=od.ODID left join
        skyhotel.`TICKET_ORDER` o on i.ORDERID=o.ORDERID
        where i.type=0
        and i.PNRSOURCE='hlth' and
        i.INCOMEDATE>=%s and i.INCOMEDATE <%s
         and o.mode=0  and od.LINKDETAILID!=0
        GROUP BY i.INCOMEDATE ;
    """

    transfer_cost_sql = """
        SELECT
        sum(case when c.COSTTYPE=0 and od.LINKTYPE is NULL then c.AMOUNT else 0 end) one_issue_cost,
        sum(case when c.COSTTYPE=1 and od.LINKTYPE is NULL then c.AMOUNT else 0 end) one_refund_cost,
        sum(case when c.COSTTYPE=0 and od.LINKTYPE=1 then c.AMOUNT else 0 end) two_issue_cost,
        sum(case when c.COSTTYPE=1 and od.LINKTYPE=1 then c.AMOUNT else 0 end) two_refund_cost
        FROM `TICKET_ORDER_COST` c join
        skyhotel.`TICKET_ORDERDETAIL` od on c.ODID=od.ODID
        left join skyhotel.`TICKET_ORDER` o on c.ORDERID=o.ORDERID
        where c.PNRSOURCE='hlth'
        and c.type=0 and c.COSTDATE>= %s and c.COSTDATE<%s
        and c.AMOUNTTYPE!=2 and o.mode=0
        and od.LINKDETAILID!=0 GROUP BY  COSTDATE;
    """

    insert_sql = """
        insert into profit_hb_self_transfer_daily
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        transfer_issue_one_income1 = values(transfer_issue_one_income1),
        transfer_issue_one_income2 = values(transfer_issue_one_income2),
        transfer_refund_one_income = values(transfer_refund_one_income),
        transfer_issue_two_income = values(transfer_issue_two_income),
        transfer_refund_two_income = values(transfer_refund_two_income),
        transfer_issue_one_cost = values(transfer_issue_one_cost),
        transfer_refund_one_cost = values(transfer_refund_one_cost),
        transfer_issue_two_cost = values(transfer_issue_two_cost),
        transfer_refund_two_cost = values(transfer_refund_two_cost)
    """
    dto = [start_date, end_date]
    income_data = DBCli().sourcedb_cli.queryOne(transfer_income_sql, dto)
    cost_data = DBCli().sourcedb_cli.queryOne(transfer_cost_sql, dto)
    insert_data = [start_date, ] + list(income_data) + list(cost_data)
    DBCli().targetdb_cli.insert(insert_sql, insert_data)
    return __file__


def update_profit_hb_supply_transfer_daily(days=0):
    """更新供应商转单收入成本, profit_hb_supply_transfer_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - days), '%Y-%m-%d')

    supply_transfer_income_sql = """
        SELECT i.INCOMEDATE, o.agentid,
        sum(case when i.INCOMEITEM=0 and od.LINKTYPE is NULL then i.INCOME else 0 end) one_issue_income1,
        sum(case when i.INCOMEITEM=0 and od.LINKTYPE is NULL then od.REALPRICE +  od.AIRPORTFEE else 0 end) * 0.005 one_issue_income2,
        sum(case when i.INCOMEITEM=1 and od.LINKTYPE is NULL then i.INCOME else 0 end) one_refund_income,
        sum(case when i.INCOMEITEM=0 and od.LINKTYPE=1 then i.INCOME else 0 end) two_issue_income,
        sum(case when i.INCOMEITEM=1 and od.LINKTYPE=1 then i.INCOME else 0 end) two_refund_income
        FROM `TICKET_ORDER_INCOME` i
        join skyhotel.`TICKET_ORDERDETAIL` od
        on i.TICKETNO=od.ODID left join
        skyhotel.`TICKET_ORDER` o on i.ORDERID=o.ORDERID
        where i.type=0
        and i.PNRSOURCE='supply' and
        i.INCOMEDATE>=%s and i.INCOMEDATE <%s
         and o.mode=0  and od.LINKDETAILID!=0
        GROUP BY i.INCOMEDATE, o.agentid ;
    """

    supply_transfer_cost_sql = """
        SELECT
        sum(case when c.COSTTYPE=0 and od.LINKTYPE is NULL then c.AMOUNT else 0 end) one_issue_cost,
        sum(case when c.COSTTYPE=1 and od.LINKTYPE is NULL then c.AMOUNT else 0 end) one_refund_cost,
        sum(case when c.COSTTYPE=0 and od.LINKTYPE=1 then c.AMOUNT else 0 end) two_issue_cost,
        sum(case when c.COSTTYPE=1 and od.LINKTYPE=1 then c.AMOUNT else 0 end) two_refund_cost,
        COSTDATE, o.agentid
        FROM `TICKET_ORDER_COST` c join
        skyhotel.`TICKET_ORDERDETAIL` od on c.ODID=od.ODID
        left join skyhotel.`TICKET_ORDER` o on c.ORDERID=o.ORDERID
        where c.PNRSOURCE='supply'
        and c.type=0 and c.COSTDATE>= %s and c.COSTDATE<%s
        and c.AMOUNTTYPE!=2 and o.mode=0
        and od.LINKDETAILID!=0 GROUP BY COSTDATE, o.agentid;
    """

    insert_sql = """
        insert into profit_hb_supply_transfer_daily
        (s_day, agentid, transfer_issue_one_income1, transfer_issue_one_income2,
        transfer_refund_one_income, transfer_issue_two_income, transfer_refund_two_income,
        createtime, updatetime)
        VALUES (%s, %s, %s, %s, %s, %s, %s, now(), now())
    """

    update_sql = """
        update profit_hb_supply_transfer_daily set transfer_issue_one_cost=%s,
        transfer_refund_one_cost = %s, transfer_issue_two_cost=%s, transfer_refund_two_cost=%s
        where s_day=%s and agentid=%s
    """
    dto = [start_date, end_date]
    income_data = DBCli().sourcedb_cli.queryAll(supply_transfer_income_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_sql, income_data)
    cost_data = DBCli().sourcedb_cli.queryAll(supply_transfer_cost_sql, dto)
    DBCli().targetdb_cli.batchInsert(update_sql, cost_data)
    return __file__


def update_profit_hb_supply_no_transfer_daily(days=0):
    """更新供应商非转单收入成本, profit_hb_supply_no_transfer_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1-days), '%Y-%m-%d')
    supply_no_transfer_income_sql = """
        SELECT i.INCOMEDATE, o.agentid,
        sum(case when i.INCOMEITEM=0 then i.INCOME else 0 end) issue_income,
        sum(case when i.INCOMEITEM=1 then i.INCOME else 0 end) refund_income
        FROM `TICKET_ORDER_INCOME` i join
        skyhotel.`TICKET_ORDERDETAIL` od on i.TICKETNO=od.ODID
        left join skyhotel.`TICKET_ORDER` o on i.ORDERID=o.ORDERID
        where i.type=0 and i.PNRSOURCE='supply'
        and i.INCOMEDATE >=%s and i.INCOMEDATE < %s
        and od.LINKTYPE is NULL and o.mode=0  and od.LINKDETAILID=0
        and o.agentid is not null
        GROUP BY i.INCOMEDATE, o.agentid
    """
    supply_no_transfer_cost_sql = """
        SELECT
        sum(case when c.COSTTYPE=0 then c.amount else 0 end) issue_cost,
        sum(case when c.COSTTYPE=1 then c.amount else 0 end) refund_cost,
        COSTDATE, o.agentid
        FROM `TICKET_ORDER_COST` c join skyhotel.`TICKET_ORDERDETAIL`
        od on c.ODID=od.ODID left join skyhotel.`TICKET_ORDER` o on c.ORDERID=o.ORDERID
        where c.PNRSOURCE='supply' and c.type=0
        and c.COSTDATE >= %s and  c.COSTDATE < %s
        and c.AMOUNTTYPE!=2   and od.LINKTYPE is NULL and o.mode=0
        and od.LINKDETAILID=0
        GROUP BY COSTDATE, o.agentid;
    """

    insert_sql = """
        insert into profit_hb_supply_no_transfer_daily (s_day, agentid,
        no_transfer_issue_income, no_transfer_refund_income,
        createtime, updatetime) values (%s, %s, %s, %s, now(), now())
    """

    update_sql = """
        update profit_hb_supply_no_transfer_daily set no_transfer_issue_cost=%s,
        no_transfer_refund_cost=%s where s_day=%s and agentid=%s
    """
    dto = [start_date, end_date]
    income_data = DBCli().sourcedb_cli.queryAll(supply_no_transfer_income_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_sql, income_data)
    cost_data = DBCli().sourcedb_cli.queryAll(supply_no_transfer_cost_sql, dto)
    DBCli().targetdb_cli.batchInsert(update_sql, cost_data)

    return __file__


if __name__ == "__main__":
    update_hbgj_income_issue_refund_daily(1)
    i = 1
    # while i <= 5:
    #     update_hbgj_cost_type_daily(i)
    #     i += 1
    # i = 1
    # while i <= 6:
    #     update_hbgj_cost_type_daily(i)
    #     i += 1
    # i = 1
    # while i <= 113:
    #     update_hbgj_transfer_order_income_cost_daily(i)
    #     update_hbgj_no_transfer_order_income_cost_daily(i)
    #     update_hbgj_supply_no_transfer_order_income_cost_daily(i)
    #     update_hbgj_supply_transfer_order_income_cost_daily(i)
    #     i += 1
    pass
    # update_hbgj_no_transfer_order_income_cost_daily(1)
    # update_hbgj_transfer_order_income_cost_daily(1)
    # update_hbgj_supply_transfer_order_income_cost_daily(1)
    # update_hbgj_supply_no_transfer_order_income_cost_daily(1)