# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from collections import defaultdict


def update_hb_channel_ticket_daily(days=0):
    """航班各个渠道票量与交易额, operation_hbgj_channel_ticket_daily"""
    start_week = DateUtil.get_date_before_days(days * 1)
    end_week = DateUtil.get_date_after_days(1 - days)
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
    channel_all_data = DBCli().sourcedb_cli.query_all(channel_sql, [start_week, start_week, end_week])
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
        elif saletype == 13 or pn_resouce == 'hlth' or saletype == 23:
            sale_data += 1
            new_channel_data.append(5)
        else:
            continue
        insert_channel_data.append(new_channel_data)

    DBCli().targetdb_cli.batch_insert(insert_sql, insert_channel_data)
    if sale_data == 0:
        DBCli().targetdb_cli.insert(do_sale_exception_sql, [start_week,  u'航班管家', 'HBGJ'])

    pass


def update_hb_channel_ticket_income_daily(days=0):
    """航班各个渠道机票收入, operation_hbgj_channel_ticket_income_daily"""
    start_date = DateUtil.get_date_before_days(days * 1)
    end_date = DateUtil.get_date_after_days(1 - days)
    sql = """
        select a.INCOMEDATE, b.SALETYPE, b.NAME, a.PNRSOURCE, SUM(INCOME),
        TICKET_ORDER.agentid from TICKET_ORDER_INCOME a
        left join PNRSOURCE_CONFIG b ON a.PNRSOURCE = b.PNRSOURCE
        left join TICKET_ORDER ON a.ORDERID = TICKET_ORDER.ORDERID
        where a.INCOMEDATE>=%s and a.INCOMEDATE<%s
        and a.TYPE=0
        and b.SALETYPE is not null
        GROUP BY a.PNRSOURCE, a.INCOMEDATE, TICKET_ORDER.agentid
    """

    supplier_sql = """
        select concat(supplier_id), supplier_name
        from supplier.sys_supplier
        where channeltype=1
        and supplier_name!='杰成'
    """

    supplier_data = dict(DBCli().targetdb_cli.query_all(supplier_sql))

    do_sale_exception_sql = """
        insert into operation_hbgj_channel_ticket_daily (s_day, saletype, channel_name, pn_resouce, ticket_num, amount, pid,
        createtime, updatetime) values (%s, 13, %s, %s, 0, 0, 5, now(), now())
    """
    dto = [DateUtil.date2str(start_date, '%Y-%m-%d'), DateUtil.date2str(end_date, '%Y-%m-%d')]

    income_data = DBCli().sourcedb_cli.query_all(sql, dto)

    insert_sql = """
        insert into operation_hbgj_channel_ticket_income_daily (s_day, saletype, channel_name, pn_resouce,
        income_amount, pid,
        createtime, updatetime) values (%s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        saletype = values(saletype),
        channel_name = values(channel_name),
        pn_resouce = values(pn_resouce),
        income_amount = values(income_amount),
        pid = values(pid)
    """
    insert_channel_data = []
    sale_data = 0

    pn_resouce_amount_dict = {}
    for channel_data in income_data:
        new_channel_data = list(channel_data)
        new_channel_data.insert(-1, supplier_data.get(new_channel_data[-1], None))
        saletype, pn_resouce = new_channel_data[1], new_channel_data[3]
        agaent_id = new_channel_data.pop()
        agaent_name = new_channel_data.pop()
        if saletype in (10, 11, 14):
            new_channel_data.append(1)
        elif saletype == 12 and pn_resouce != 'supply' and pn_resouce != 'hlth':
            new_channel_data.append(2)
        elif saletype in (20, 21, 22) and pn_resouce != 'intsupply':
            new_channel_data.append(3)
        elif pn_resouce == 'intsupply':
            new_channel_data.append(4)
        elif pn_resouce == "supply" and (agaent_name is not None and len(agaent_name) > 0):
            new_channel_data[2] = agaent_name
            new_channel_data[3] = agaent_id
            new_channel_data.append(4)
        elif saletype == 13 or pn_resouce == 'hlth' or saletype == 23:
            sale_data += 1
            new_channel_data.append(5)
        else:
            new_channel_data.append(2)

        if new_channel_data[3] in pn_resouce_amount_dict:
            (pn_resouce_amount_dict[new_channel_data[3]])[4] += new_channel_data[4]
        else:
            pn_resouce_amount_dict[new_channel_data[3]] = new_channel_data
        # insert_channel_data.append(new_channel_data)
    for income_k, income_v in pn_resouce_amount_dict.items():
        insert_channel_data.append(income_v)
    DBCli().targetdb_cli.batch_insert(insert_sql, insert_channel_data)
    if sale_data == 0:
        DBCli().targetdb_cli.insert(do_sale_exception_sql, [start_date, u'航班管家', 'HBGJ'])


def update_hb_company_ticket_weekly():
    """航班, operation_hbgj_company_ticket_weekly"""
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
    hb_info = DBCli().oracle_cli.query_all(hb_code_sql)
    hb_info = dict(hb_info)

    hb_company_data = DBCli().sourcedb_cli.query_all(hb_sql, [start_week, start_week, end_week])
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
    DBCli().targetdb_cli.batch_insert(insert_sql, insert_hb_company)


def update_unable_ticket():
    """无法出票和人工干预出票数量周, operation_hbgj_unable_ticket_weekly operation_hbgj_human_intervention_ticket_weekly"""
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
            SELECT PNRSOURCE_CONFIG.SALETYPE, t.PNRSOURCE, PNRSOURCE_CONFIG.NAME , COUNT(DISTINCT t.ORDERID) FROM
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
        SELECT PNRSOURCE_CONFIG.SALETYPE, O.PNRSOURCE pnrsource, PNRSOURCE_CONFIG.NAME, count(*) ticket_num,
        count(DISTINCT OD.ORDERID) order_num FROM
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
        insert into operation_hbgj_human_intervention_ticket_weekly (s_day, saletype, pid, pn_resouce, channel_name,
        intervention_ticket_num, total_num, createtime, updatetime)
        values (%s, %s, %s, %s, %s, %s, %s, now(), now())
    """
    insert_data = []
    insert_intervention_data = []
    while start_week < end_week:
        start_date = DateUtil.date2str(start_week)
        next_date = DateUtil.date2str(DateUtil.add_days(start_week, 1))
        dto = [start_date, next_date]
        unable_ticket_data = DBCli().sourcedb_cli.query_all(unable_ticket_sql, dto)

        human_intervention_data = DBCli().sourcedb_cli.query_all(human_intervention_sql, dto)
        total_ticket_data = DBCli().sourcedb_cli.query_all(total_ticket_sql, dto)

        total_t_o = defaultdict(list)

        for total in total_ticket_data:
            total_t_o[total[1].lower()] = [total[0], total[2], total[3], total[4]]

        no_unable = defaultdict(list)
        for unable_ticket in unable_ticket_data:
            tmp_unable_data = list(unable_ticket)
            pn_resource, pn_name, ticket_num = tmp_unable_data
            no_unable[pn_resource] = [pn_name, ticket_num]
            try:
                if pn_resource is None and pn_name is None:
                    tmp_unable_data = [u'空白', u'空白', ticket_num]
                    total_num = 0
                elif pn_name is None and pn_resource is not None:
                    tmp_unable_data = [pn_resource, u'默认', ticket_num]
                    total_num = 0
                else:
                    total_num = (total_t_o.get(pn_resource.lower()))[2]
            except (IndexError, AttributeError, KeyError, TypeError):
                total_num = 0

            tmp_unable_data.insert(0, DateUtil.date2str(start_week, '%Y-%m-%d'))
            tmp_unable_data.append(total_num)
            insert_data.append(tmp_unable_data)

        no_unable_list = set(total_t_o.keys()).difference(set(no_unable.keys()))

        for no_unable_key in no_unable_list:
            pn_name, ticket_num = (total_t_o[no_unable_key.lower()])[1], (total_t_o[no_unable_key.lower()])[2]
            tmp_data = [DateUtil.date2str(start_week, '%Y-%m-%d'), no_unable_key, pn_name, 0, ticket_num]
            insert_data.append(tmp_data)

        no_intervention = defaultdict(list)

        for intervention_ticket in human_intervention_data:
            tmp_data = list(intervention_ticket)

            saletype, pn_resource, pn_name, order_num = tmp_data
            no_intervention[pn_resource].extend([pn_name, order_num])
            try:
                total_num = (total_t_o.get(pn_resource.lower()))[3]
            except (IndexError, AttributeError, KeyError, TypeError):
                continue

            tmp_data.insert(0, DateUtil.date2str(start_week, '%Y-%m-%d'))

            pid = get_pid_sale_type(saletype, pn_resource)
            if pid:
                tmp_data.insert(2, pid)
            else:
                continue
            tmp_data.append(total_num)

            insert_intervention_data.append(tmp_data)

        no_intervention_list = set(total_t_o.keys()).difference(set(no_intervention.keys()))
        for no_intervention_key in no_intervention_list:
            no_saletype, pn_intervention_name, order_intervention_num = \
                total_t_o[no_intervention_key][0], total_t_o[no_intervention_key][1], total_t_o[no_intervention_key][3]
            pid = get_pid_sale_type(no_saletype, no_intervention_key)
            if not pid:
                continue
            tmp_intervention_data = [DateUtil.date2str(start_week, '%Y-%m-%d'), no_saletype, pid, no_intervention_key,
                                     pn_intervention_name, 0, order_intervention_num]
            insert_intervention_data.append(tmp_intervention_data)
        start_week = DateUtil.add_days(start_week, 1)
    DBCli().targetdb_cli.batch_insert(insert_sql, insert_data)
    DBCli().targetdb_cli.batch_insert(insert_intervention_sql, insert_intervention_data)


def get_pid_sale_type(sale_type, pn_resource):
    if sale_type in (10, 11, 14):
        return 1
    elif sale_type == 12 and pn_resource != 'supply' and pn_resource != 'hlth':
        return 2
    elif sale_type in (20, 21, 22) and pn_resource != 'intsupply':
        return 3
    elif pn_resource == 'intsupply' or pn_resource == 'supply':
        return 4
    elif sale_type == 13 or pn_resource == 'hlth' or sale_type == 23:
        return 5
    else:
        return None


def update_supplier_refused_order_weekly():
    """供应商退票订单周, operation_hbgj_company_ticket_weekly"""
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
    query_data = DBCli().sourcedb_cli.query_all(week_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)


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
        query_data = DBCli().targetdb_cli.query_one(check_sql, [min_date])
        if query_data[0] == 0:
            DBCli().targetdb_cli.insert(do_sale_exception_sql, [DateUtil.date2str(min_date, '%Y-%m-%d'), u'航班管家', 'HBGJ'])
        min_date = DateUtil.add_days(min_date, 1)


def update_product_ticket_daily(days=0):
    """供应商出票, operation_hbgj_supplier_ticket_daily"""
    # start_week, end_week = DateUtil.get_last_week_date()
    start_week = DateUtil.get_date_before_days(days * 1)
    end_week = DateUtil.get_date_after_days(1 - days)
    product_sql = """
        SELECT left(od.CREATETIME, 10) s_day, o.agentid,count(*),sum(od.OUTPAYPRICE) FROM
        `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER` o
        on od.ORDERID=o.ORDERID where od.CREATETIME
        BETWEEN %s and %s and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
        IFNULL(od.`LINKTYPE`, 0) != 2
        and agentid is not NULL and LENGTH(agentid)>0 and intflag=0
        and o.PNRSOURCE not in ('HBGJ')
        GROUP BY o.agentid, s_day order by s_day;
    """
    insert_sql = """
        insert into operation_hbgj_supplier_ticket_daily (s_day, agentid, ticket_num, amount, createtime, updatetime)
        values (%s, %s, %s, %s, now(), now())
    """
    dto = [start_week, end_week]
    supplier_data = DBCli().sourcedb_cli.query_all(product_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, supplier_data)


def update_refund_ticket_channel_daily(days=0):
    """航班渠道退票, operation_hbgj_channel_refund_ticket_daily"""
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
        insert into operation_hbgj_channel_refund_ticket_daily (s_day, pid,
        refund_ticket_num, refund_ticket_amount, ticket_num, ticket_amount,
        createtime, updatetime) values (%s, %s, %s, %s, %s, %s, now(), now())
    """
    sale_data = 0
    dto = [query_date, query_date, today]
    channel_refund_data = DBCli().sourcedb_cli.query_all(refund_channel_ticket_sql, dto)
    channel_ticket_data = DBCli().sourcedb_cli.query_all(ticket_sql, dto)

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
        elif saletype in (20, 21, 22, 24) and pn_resouce != 'intsupply':
            pid = 3
        elif pn_resouce == 'intsupply' or pn_resouce == 'supply':
            pid = 4
        elif saletype == 13 or pn_resouce == 'hlth' or saletype == 23:
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
        elif saletype in (20, 21, 22, 24) and pn_resouce != 'intsupply':
            pid = 3
        elif pn_resouce == 'intsupply' or pn_resouce == 'supply':
            pid = 4
        elif saletype == 13 or pn_resouce == 'hlth' or saletype == 23:
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

    DBCli().targetdb_cli.batch_insert(update_refund_sql, insert_data)


def update_operation_hbgj_obsolete_order_daily(days=1):
    """作废订单, operation_hbgj_obsolete_order_daily"""
    start_date = DateUtil.get_date_before_days(days * 5)
    end_date = DateUtil.get_date_after_days(1 - days)
    dto = [DateUtil.date2str(start_date, '%Y-%m-%d'), DateUtil.date2str(end_date, '%Y-%m-%d')]
    obsolete_sql = """
        SELECT DATE_FORMAT(od.CREATETIME, '%%Y-%%m-%%d') s_day, o.PNRSOURCE, PC.name,
        count(DISTINCT(o.ORDERID)) FROM `TICKET_ORDERDETAIL` od
        INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        left join PNRSOURCE_CONFIG PC ON o.PNRSOURCE=PC.PNRSOURCE where
        od.CREATETIME >= %s and od.CREATETIME < %s  AND
        o.ORDERSTATUE=12 GROUP BY o.PNRSOURCE, PC.name, s_day
    """

    total_ticket_sql = """
        SELECT DATE_FORMAT(OD.CREATETIME, '%%Y-%%m-%%d') s_day,
        O.PNRSOURCE pnrsource, PNRSOURCE_CONFIG.NAME,
        count(DISTINCT OD.ORDERID) order_num FROM
        TICKET_ORDERDETAIL OD
        LEFT JOIN TICKET_ORDER O ON OD.ORDERID = O.ORDERID
        INNER join PNRSOURCE_CONFIG ON
        O.PNRSOURCE=PNRSOURCE_CONFIG.PNRSOURCE
        WHERE
        O.ORDERSTATUE  NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
        IFNULL(OD.`LINKTYPE`, 0) != 2
        AND OD.CREATETIME >= %s
        AND OD.CREATETIME < %s
        GROUP BY O.PNRSOURCE, PNRSOURCE_CONFIG.NAME, s_day
    """

    insert_sql = """
        insert into operation_hbgj_obsolete_order_daily (s_day, pn_resouce, channel_name, obsolete_order_num,
        total_order_num, createtime, updatetime) values (%s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        obsolete_order_num = values(obsolete_order_num),
        total_order_num = values(total_order_num)
    """

    obsolete_data = DBCli().sourcedb_cli.query_all(obsolete_sql, dto)
    total_data = DBCli().sourcedb_cli.query_all(total_ticket_sql, dto)
    total_data_dict = defaultdict(list)
    no_obsolete = defaultdict(list)
    insert_data = []
    for t_d in total_data:
        total_data_dict[t_d[0] + ":" + t_d[1]] = [t_d[2], t_d[3]]

    for ob_data in obsolete_data:
        s_day, pn, pn_name, obsolete_order_num = ob_data
        no_obsolete[s_day + ":" + pn] = []
        try:
            total_order = (total_data_dict[s_day + ":" + pn])[1]
        except (IndexError, AttributeError, KeyError, TypeError):
            continue
        insert_data.append([s_day, pn, pn_name, obsolete_order_num, total_order])
    no_obsolete_list = set(total_data_dict.keys()).difference(set(no_obsolete.keys()))

    for no_ob_key in no_obsolete_list:
        s_day, pn = no_ob_key.split(":")
        insert_data.append([s_day, pn, total_data_dict[no_ob_key][0], 0, total_data_dict[no_ob_key][1]])

    DBCli().targetdb_cli.batch_insert(insert_sql, insert_data)


if __name__ == "__main__":
    # i = 1
    # while i <= 352:
    #     update_hb_channel_ticket_income_daily(i)
    #     i += 1
    update_product_ticket_daily(1)