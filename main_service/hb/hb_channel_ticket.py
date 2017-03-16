# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_hb_channel_ticket_weekly():
    start_week, end_week = DateUtil.get_last_week_date()
    channel_sql = """
        select %s, PNRSOURCE_CONFIG.SALETYPE, PNRSOURCE_CONFIG.NAME,A.pn_resouce, A.ticket_num, A.amount from (
        SELECT o.PNRSOURCE pn_resouce,count(*) ticket_num,sum(od.OUTPAYPRICE) amount FROM
        `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER` o
        on od.ORDERID=o.ORDERID where od.CREATETIME
        BETWEEN %s and %s and
        o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
         IFNULL(od.`LINKTYPE`, 0) != 2 GROUP BY o.PNRSOURCE) A
        left join PNRSOURCE_CONFIG ON A.pn_resouce=PNRSOURCE_CONFIG.PNRSOURCE
    """

    insert_sql = """
        insert into operation_hbgj_channel_ticket_daily (s_day, saletype, channel_name, pn_resouce, ticket_num, amount, pid,
        createtime, updatetime) values (%s, %s, %s, %s, %s, %s, %s, now(), now())
    """

    do_sale_exception_sql = """
        insert into operation_hbgj_channel_ticket_daily (s_day, saletype, channel_name, pn_resouce, ticket_num, amount, pid,
        createtime, updatetime) values (%s, 13, %s, %s, 0, 0, 5, now(), now())
    """

    while start_week < end_week:
        query_end_date = DateUtil.add_days(start_week, 1)
        channel_all_data = DBCli().sourcedb_cli.queryAll(channel_sql, [start_week, start_week, query_end_date])
        # new_channel_data = [list(channel_data).insert(0, s_day) for channel_data in channel_all_data]

        insert_channel_data = []
        sale_data = 0
        for channel_data in channel_all_data:
            saletype, pn_resouce = channel_data[1], channel_data[3]
            new_channel_data = list(channel_data)
            if saletype in (10, 11, 14):
                new_channel_data.append(1)
            elif saletype == 12 and pn_resouce != 'supply' and pn_resouce != 'hlth':
                new_channel_data.append(2)
            elif saletype in (20, 21, 22) and pn_resouce != 'intsupply':
                new_channel_data.append(3)
            elif pn_resouce == 'intsupply' or pn_resouce == 'supply':
                new_channel_data.append(4)
            elif saletype == 13 or pn_resouce == 'hlth':
                sale_data += 1
                new_channel_data.append(5)
            else:
                continue
            insert_channel_data.append(new_channel_data)

        DBCli().targetdb_cli.batchInsert(insert_sql, insert_channel_data)
        if sale_data == 0:
            DBCli().targetdb_cli.insert(do_sale_exception_sql, [start_week,  u'航班管家', 'HBGJ'])
        start_week = DateUtil.add_days(start_week, 1)


    # total_week_sql = """
    #     select
    #     s_day,
    #     sum(case when saletype in (10, 11, 14) then ticket_num else 0 end) hb_company_channel_ticket,
    #     sum(case when saletype in (10, 11, 14) then amount else 0 end) hb_company_channel_amount,
    #
    #     sum(case when saletype in (12) and pn_resouce != 'supply' then ticket_num else 0 end) partner_ticket,
    #     sum(case when saletype in (12) and pn_resouce != 'supply' then amount else 0 end) partner_amount,
    #
    #     sum(case when saletype in (20, 21, 22) and pn_resouce != 'intsupply' then ticket_num else 0 end) inter_channel_ticket,
    #     sum(case when saletype in (20, 21, 22) and pn_resouce != 'intsupply' then amount else 0 end) inter_channel_amount,
    #
    #     sum(case when pn_resouce = 'intsupply' and pn_resouce = 'supply' then ticket_num else 0 end) provider_channel_ticket,
    #     sum(case when pn_resouce = 'intsupply' and pn_resouce = 'supply' then amount else 0 end) provider_channel_amount,
    #
    #     sum(case when saletype = 13 and pn_resouce = 'hlth' then ticket_num else 0 end) selfsupport_channel_ticket,
    #     sum(case when saletype = 13 and pn_resouce = 'hlth' then amount else 0 end) selfsupport_channel_amount
    #     from hbgj_channel_ticket_weekly where s_day=%s
    # """
    #
    # insert_total_sql = """
    #     insert into hbgj_channel_class_ticket_weekly (s_day, hb_company_channel_ticket, hb_company_channel_amount, partner_ticket,
    #     partner_amount, inter_channel_ticket, inter_channel_amount, provider_channel_ticket, provider_channel_amount, selfsupport_channel_ticket,
    #     selfsupport_channel_amount, createtime, updatetime) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
    # """
    # total_week_data = DBCli().targetdb_cli.queryOne(total_week_sql, [s_day])
    # DBCli().targetdb_cli.insert(insert_total_sql, total_week_data)


def update_hb_company_ticket_weekly():
    import os
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    start_week, end_week = DateUtil.get_last_week_date()
    hb_sql = """
        SELECT %s, SUBSTR(`flyno`,1,2),count(*),sum(od.OUTPAYPRICE)
        FROM `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER`
        o on od.ORDERID=o.ORDERID where od.CREATETIME
        BETWEEN %s and %s and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
        IFNULL(od.`LINKTYPE`, 0) != 2  GROUP BY SUBSTR(`flyno`,1,2) ORDER BY count(*) desc
    """

    hb_code_sql = """
        select code,FOUR_NAME
        from AIRLINES_NORMAl
    """
    hb_info = DBCli().oracle_cli.queryAll(hb_code_sql)
    hb_info = dict(hb_info)

    hb_company_data = DBCli().sourcedb_cli.queryAll(hb_sql, [start_week, start_week, end_week])
    insert_hb_company = []
    for hb_data in hb_company_data:
        if hb_data[1] in hb_info:
            hb_data = list(hb_data)
            hb_data.insert(1, hb_info[hb_data[1]])
            insert_hb_company.append(hb_data)
        else:
            hb_data = list(hb_data)
            hb_data.insert(1, None)
            insert_hb_company.append(hb_data)

    insert_sql = """
        insert into operation_hbgj_company_ticket_weekly (s_day, hb_comany, hb_code, ticket_num, amount, createtime, updatetime)
        values (%s, %s, %s, %s, %s, now(), now())
    """
    DBCli().targetdb_cli.batchInsert(insert_sql, insert_hb_company)


def update_unable_ticket():
    start_week, end_week = DateUtil.get_last_week_date()
    unable_ticket_sql = """
    select a_table.oldsource, a_table.name, sum(a_table.ticket_num * a_table.AMOUNT)/50
    from
    (select A.oldsource, PNRSOURCE_CONFIG.NAME, A.ticket_num, A.AMOUNT
    from (SELECT count(*) ticket_num,o.oldsource, r.AMOUNT FROM RECHARGE_RECORD
    as r INNER JOIN TICKET_ORDER AS o ON r.orderid=o.ORDERID
       where r.RECHARGE_TYPE=8
       AND r.REMARK LIKE '无法出票补偿%%'
       and r.RECHARGE_TIME >=%s
       and r.RECHARGE_TIME < %s
       GROUP BY o.oldsource,r.AMOUNT) as A
       left join PNRSOURCE_CONFIG ON
       A.oldsource=PNRSOURCE_CONFIG.PNRSOURCE
    ) a_table group by a_table.name, a_table.oldsource
    """

    human_intervention_sql = """
            SELECT t.PNRSOURCE, PNRSOURCE_CONFIG.NAME , COUNT(DISTINCT t.ORDERID) FROM
            FLIGHT_TASK f
            LEFT JOIN TICKET_ORDER t ON f.ORDERID = t.ORDERID
            left join PNRSOURCE_CONFIG ON t.PNRSOURCE=
            PNRSOURCE_CONFIG.PNRSOURCE
            WHERE
            f.CREATE_TIME >= %s
            AND f.CREATE_TIME < %s
            AND f.TASKTYPE = 4
            AND f.DISABLED != 1
            AND f.DONE_USERID !=1
            AND f.DONE_USERID is not NULL
            GROUP BY t.PNRSOURCE, PNRSOURCE_CONFIG.NAME

    """

    total_ticket_sql = """
        SELECT O.PNRSOURCE pnrsource, PNRSOURCE_CONFIG.NAME, count(*) ticket_num, count(DISTINCT OD.ORDERID) order_num FROM
        TICKET_ORDERDETAIL OD
        LEFT JOIN TICKET_ORDER O ON OD.ORDERID = O.ORDERID
        INNER join PNRSOURCE_CONFIG ON
        O.PNRSOURCE=PNRSOURCE_CONFIG.PNRSOURCE
        WHERE
        O.ORDERSTATUE  NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
        IFNULL(OD.`LINKTYPE`, 0) != 2
        AND OD.CREATETIME >= %s
        AND OD.CREATETIME < %s
        GROUP BY O.PNRSOURCE, PNRSOURCE_CONFIG.NAME
    """

    insert_sql = """
        insert into operation_hbgj_unable_ticket_weekly (s_day, pn_resouce,
        channel_name, unable_ticket_num, total_num, createtime, updatetime)
        values (%s, %s, %s, %s, %s, now(), now())
    """

    insert_intervention_sql = """
        insert into operation_hbgj_human_intervention_ticket_weekly (s_day, pn_resouce, channel_name, intervention_ticket_num,
        total_num, createtime, updatetime)
        values (%s, %s, %s, %s, %s, now(), now())
    """
    insert_data = []
    from collections import defaultdict
    insert_intervention_data = []
    while start_week < end_week:
        start_date = DateUtil.date2str(start_week)
        next_date = DateUtil.date2str(DateUtil.add_days(start_week, 1))
        dto = [start_date, next_date]
        unable_ticket_data = DBCli().sourcedb_cli.queryAll(unable_ticket_sql, dto)

        human_intervention_data = DBCli().sourcedb_cli.queryAll(human_intervention_sql, dto)
        total_ticket_data = DBCli().sourcedb_cli.queryAll(total_ticket_sql, dto)

        total_t_o = defaultdict(list)

        for total in total_ticket_data:
            total_t_o[total[0].lower()] = [total[1], total[2], total[3]]

        no_unable = defaultdict(list)
        for unable_ticket in unable_ticket_data:
            tmp_unable_data = list(unable_ticket)
            pn_resource, pn_name, ticket_num = tmp_unable_data
            no_unable[pn_resource] = [pn_name, ticket_num]
            try:
                # total_num = total_t_o[pn_resource.lower()][1]
                if pn_resource is None and pn_name is None:
                    tmp_unable_data = [u'空白', u'空白', ticket_num]
                    total_num = 0
                elif pn_name is None and pn_resource is not None:
                    tmp_unable_data = [pn_resource, u'默认', ticket_num]
                    total_num = 0
                else:
                    total_num = (total_t_o.get(pn_resource.lower()))[1]
            except (IndexError, AttributeError, KeyError, TypeError):
                total_num = 0

            tmp_unable_data.insert(0, DateUtil.date2str(start_week, '%Y-%m-%d'))
            tmp_unable_data.append(total_num)
            insert_data.append(tmp_unable_data)

        no_unable_list = set(total_t_o.keys()).difference(set(no_unable.keys()))

        for no_unable_key in no_unable_list:
            pn_name, ticket_num = (total_t_o[no_unable_key.lower()])[0], (total_t_o[no_unable_key.lower()])[1]
            tmp_data = [DateUtil.date2str(start_week, '%Y-%m-%d'), no_unable_key, pn_name, 0, ticket_num]
            insert_data.append(tmp_data)
        print insert_data
        no_intervention = defaultdict(list)

        for intervention_ticket in human_intervention_data:
            tmp_data = list(intervention_ticket)
            pn_resource, pn_name, order_num = tmp_data
            no_intervention[pn_resource].extend([pn_name, order_num])
            try:
                total_num = (total_t_o.get(pn_resource.lower()))[2]
            except (IndexError, AttributeError, KeyError, TypeError):
                continue
            tmp_data.insert(0, DateUtil.date2str(start_week, '%Y-%m-%d'))
            tmp_data.append(total_num)
            insert_intervention_data.append(tmp_data)

        no_intervention_list = set(total_t_o.keys()).difference(set(no_intervention.keys()))
        for no_intervention_key in no_intervention_list:
            pn_intervention_name, order_intervention_num = total_t_o[no_intervention_key][0], \
                                                           total_t_o[no_intervention_key][2]
            tmp_intervention_data = [DateUtil.date2str(start_week, '%Y-%m-%d'), no_intervention_key,
                                     pn_intervention_name, 0, order_intervention_num]
            insert_intervention_data.append(tmp_intervention_data)
        start_week = DateUtil.add_days(start_week, 1)
    DBCli().targetdb_cli.batchInsert(insert_sql, insert_data)
    DBCli().targetdb_cli.batchInsert(insert_intervention_sql, insert_intervention_data)


def update_supplier_refused_order_weekly():
    start_week, end_week = DateUtil.get_last_week_date()
    week_sql = """
            select a_table.s_day, a_table.agentid, b_table.order_num,
            a_table.refused_order, a_table.over_time_order from (
            SELECT %s s_day, agentid,
            count(DISTINCT case when f.DESCRIPTION like '%%拒单%%' then t.ORDERID end) refused_order,
            count(DISTINCT case when f.DESCRIPTION like '%%出票时间超过%%' then t.ORDERID end) over_time_order
            FROM
            FLIGHT_TASK f
            LEFT JOIN TICKET_ORDER t ON f.ORDERID = t.ORDERID
            WHERE
            f.CREATE_TIME >= %s
            AND f.CREATE_TIME < %s
            AND f.TASKTYPE = 4
            AND f.DISABLED != 1
            AND f.DONE_USERID !=1
            AND f.DONE_USERID is not NULL
            and t.PNRSOURCE='supply'
            and agentid is not null
            GROUP BY agentid) AS a_table
            right join
            (
            SELECT %s s_day,
             agentid,count( DISTINCT O.ORDERID ) order_num
            FROM
            TICKET_ORDERDETAIL OD
            LEFT JOIN TICKET_ORDER O ON OD.ORDERID = O.ORDERID
            WHERE
            O.ORDERSTATUE  NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
             IFNULL(OD.`LINKTYPE`, 0) != 2
            AND OD.CREATETIME >= %s
            AND OD.CREATETIME < %s
            and O.PNRSOURCE='supply'
            and agentid is not null
            GROUP BY  agentid) as b_table on a_table.s_day=b_table.s_day
            and a_table.agentid = b_table.agentid
    """

    insert_sql = """
        insert into operation_hbgj_supplier_refused_weekly (s_day, agentid, order_num,
        refused_order_num, overtime_order_num, createtime, updatetime)
        values (%s, %s, %s, %s, %s, now(), now())
    """
    dto = [DateUtil.date2str(start_week, '%Y-%m-%d'), start_week,
           end_week, DateUtil.date2str(start_week, '%Y-%m-%d'), start_week, end_week]
    query_data = DBCli().sourcedb_cli.queryAll(week_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_sql, query_data)


def do_exception_sale():
    do_sale_exception_sql = """
        insert into operation_hbgj_channel_ticket_daily (s_day, saletype, channel_name, pn_resouce, ticket_num, amount, pid,
        createtime, updatetime) values (%s, 13, %s, %s, 0, 0, 5, now(), now())
    """

    check_sql = """
        select count(*)
        from operation_hbgj_channel_ticket_daily
        where s_day=%s
        and pid=5
    """
    import datetime
    min_date = datetime.date(2013, 2, 19)
    end_date = datetime.date(2017, 2, 27)
    while min_date < end_date:
        query_data = DBCli().targetdb_cli.queryOne(check_sql, [min_date])
        if query_data[0] == 0:
            DBCli().targetdb_cli.insert(do_sale_exception_sql, [DateUtil.date2str(min_date, '%Y-%m-%d'), u'航班管家', 'HBGJ'])
        min_date = DateUtil.add_days(min_date, 1)


def update_product_ticket_weekly():
    start_week, end_week = DateUtil.get_last_week_date()
    product_sql = """
        SELECT left(od.CREATETIME, 10) s_day, o.agentid,count(*),sum(od.OUTPAYPRICE) FROM
        `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER` o
        on od.ORDERID=o.ORDERID where od.CREATETIME
        BETWEEN %s and %s and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
         IFNULL(od.`LINKTYPE`, 0) != 2 and o.PNRSOURCE='supply' GROUP BY o.agentid, s_day order by s_day;
    """
    insert_sql = """
        insert into operation_hbgj_supplier_ticket_daily (s_day, agentid, ticket_num, amount, createtime, updatetime)
        values (%s, %s, %s, %s, now(), now())
    """
    dto = [start_week, end_week]
    supplier_data = DBCli().sourcedb_cli.queryAll(product_sql, dto)
    DBCli().targetdb_cli.batchInsert(insert_sql, supplier_data)


def update_refund_ticket_channel_daily(days=0):
    query_date = DateUtil.get_date_before_days(days * 1)
    today = DateUtil.get_date_after_days(1 - days)

    refund_channel_ticket_sql = """
        select A.s_day,PNRSOURCE_CONFIG.SALETYPE, PNRSOURCE_CONFIG.NAME,A.pn_resouce, A.back_num, A.amount from (
        SELECT %s s_day, o.PNRSOURCE pn_resouce, count(*) back_num, sum(od.OUTPAYPRICE) amount
        FROM skyhotel.`TICKET_ORDERDETAIL` od
        INNER JOIN skyhotel.`TICKET_ORDER` o on od.ORDERID=o.ORDERID
        INNER JOIN skyhotel.`TICKET_ORDER_REFUND` r on od.ORDERID=r.ORDERID
        where r.createtime
        BETWEEN %s and %s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31)
        AND IFNULL(od.`LINKTYPE`, 0) != 2 and  IFNULL(od.REFUNDID,0) != 0
        GROUP BY o.PNRSOURCE) A
        left join PNRSOURCE_CONFIG ON A.pn_resouce=PNRSOURCE_CONFIG.PNRSOURCE
    """

    ticket_sql = """
        select %s s_day, PNRSOURCE_CONFIG.SALETYPE, PNRSOURCE_CONFIG.NAME,AA.pn_resouce, AA.ticket_num, AA.amount from (
        SELECT o.PNRSOURCE pn_resouce,count(*) ticket_num,sum(od.OUTPAYPRICE) amount FROM
        `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER` o
        on od.ORDERID=o.ORDERID where od.CREATETIME
        BETWEEN %s and %s  and
        o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
         IFNULL(od.`LINKTYPE`, 0) != 2 GROUP BY o.PNRSOURCE) AA
        left join PNRSOURCE_CONFIG ON AA.pn_resouce=PNRSOURCE_CONFIG.PNRSOURCE
    """
    update_refund_sql = """
        insert operation_hbgj_channel_refund_ticket_daily (s_day, pid,
        refund_ticket_num, refund_ticket_amount, ticket_num, ticket_amount,
        createtime, updatetime) values (%s, %s, %s, %s, %s, %s, now(), now())
    """
    sale_data = 0
    dto = [query_date, query_date, today]
    channel_refund_data = DBCli().sourcedb_cli.queryAll(refund_channel_ticket_sql, dto)
    channel_ticket_data = DBCli().sourcedb_cli.queryAll(ticket_sql, dto)

    from collections import defaultdict
    pid_channel_refund_ticket_num = defaultdict(list)
    pid_channel_refund_ticket_amount = defaultdict(list)
    pid_channel_ticket_num = defaultdict(list)
    pid_channel_ticket_amount = defaultdict(list)
    insert_data = []

    for channel_data in channel_refund_data:
        s_day, saletype, pn_name, pn_resouce, refund_ticket_num, refund_ticket_amount = channel_data
        if saletype in (10, 11, 14):
            pid = 1
        elif saletype == 12 and pn_resouce != 'supply' and pn_resouce != 'hlth':
            pid = 2
        elif saletype in (20, 21, 22) and pn_resouce != 'intsupply':
            pid = 3
        elif pn_resouce == 'intsupply' or pn_resouce == 'supply':
            pid = 4
        elif saletype == 13 or pn_resouce == 'hlth':
            sale_data += 1
            pid = 5
        else:
            continue
        pid_channel_refund_ticket_num[pid].append(refund_ticket_num)
        pid_channel_refund_ticket_amount[pid].append(refund_ticket_amount)

    for channel_data in channel_ticket_data:
        s_day, saletype, pn_name, pn_resouce, ticket_num, ticket_amount \
            = channel_data
        if saletype in (10, 11, 14):
            pid = 1
        elif saletype == 12 and pn_resouce != 'supply' and pn_resouce != 'hlth':
            pid = 2
        elif saletype in (20, 21, 22) and pn_resouce != 'intsupply':
            pid = 3
        elif pn_resouce == 'intsupply' or pn_resouce == 'supply':
            pid = 4
        elif saletype == 13 or pn_resouce == 'hlth':
            sale_data += 1
            pid = 5
        else:
            continue
        pid_channel_ticket_num[pid].append(ticket_num)
        pid_channel_ticket_amount[pid].append(ticket_amount)

    for i in xrange(1, 6, 1):
        new_insert_data = [query_date, i,
                           sum(pid_channel_refund_ticket_num.get(i, [0])), sum(pid_channel_refund_ticket_amount.get(i, [0.0])),
                           sum(pid_channel_ticket_num.get(i, [0])), sum(pid_channel_ticket_amount.get(i, [0.0]))]
        insert_data.append(new_insert_data)

    DBCli().targetdb_cli.batchInsert(update_refund_sql, insert_data)


if __name__ == "__main__":
    import datetime
    # update_product_ticket_weekly()
    #
    # hb_code_sql = """
    #         select code,FOUR_NAME
    #         from AIRLINES_NORMAl
    #     """
    # hb_info = DBCli().oracle_cli.queryAll(hb_code_sql)
    # hb_info = dict(hb_info)

    # start_date = datetime.date(2017, 3, 13)
    # end_date = datetime.date(2015, 4, 6)
    # # end_date = datetime.date(2017, 3, 6)
    # while start_date > end_date:
    #     start_week, end_week = DateUtil.get_this_week_date(end_date)
    #     update_refund_ticket_channel(start_week, end_week)
    #     print start_week, end_week
    #     end_date = end_week
    i = 1
    #1585
    while i <= 1585:
        update_refund_ticket_channel_daily(i)
        i += 1