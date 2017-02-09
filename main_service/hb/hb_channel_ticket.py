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
        insert into hbgj_channel_ticket_daily (s_day, saletype, channel_name, pn_resouce, ticket_num, amount, pid,
        createtime, updatetime) values (%s, %s, %s, %s, %s, %s, %s, now(), now())
    """

    while start_week < end_week:
        query_end_date = DateUtil.add_days(start_week, 1)
        channel_all_data = DBCli().sourcedb_cli.queryAll(channel_sql, [start_week, start_week, query_end_date])
        # new_channel_data = [list(channel_data).insert(0, s_day) for channel_data in channel_all_data]
        insert_channel_data = []
        for channel_data in channel_all_data:
            saletype, pn_resouce = channel_data[1], channel_data[3]
            new_channel_data = list(channel_data)
            if saletype in (10, 11, 14):
                new_channel_data.append(1)
            elif saletype == 12 and pn_resouce != 'supply':
                new_channel_data.append(2)
            elif saletype in (20, 21, 22) and pn_resouce != 'intsupply':
                new_channel_data.append(3)
            elif pn_resouce == 'intsupply' or pn_resouce == 'supply':
                new_channel_data.append(4)
            elif saletype == 13 or pn_resouce == 'hlth':
                new_channel_data.append(5)
            else:
                continue
            insert_channel_data.append(new_channel_data)
        DBCli().targetdb_cli.batchInsert(insert_sql, insert_channel_data)
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
    start_week, end_week = DateUtil.get_last_week_date()
    hb_sql = """
        SELECT %s, SUBSTR(`flyno`,1,2),count(*),sum(od.OUTPAYPRICE)
        FROM `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER`
        o on od.ORDERID=o.ORDERID where od.CREATETIME
        BETWEEN %s and %s and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
        IFNULL(od.`LINKTYPE`, 0) != 2  GROUP BY SUBSTR(`flyno`,1,2) ORDER BY count(*) desc
    """

    hb_code_sql = """
        select code, name from airline_code
    """
    hb_info = DBCli().targetdb_cli.queryAll(hb_code_sql)
    hb_info = dict(hb_info)

    hb_company_data = DBCli().sourcedb_cli.queryAll(hb_sql, [start_week, start_week, end_week])
    insert_hb_company = []
    for hb_data in hb_company_data:
        if hb_data[1] in hb_info:
            hb_data = list(hb_data)
            hb_data.insert(1, hb_info[hb_data[1]])
            insert_hb_company.append(hb_data)

    insert_sql = """
        insert into hbgj_company_ticket_weekly (s_day, hb_comany, hb_code, ticket_num, amount, createtime, updatetime)
        values (%s, %s, %s, %s, %s, now(), now())
    """
    DBCli().targetdb_cli.batchInsert(insert_sql, insert_hb_company)

# if __name__ == "__main__":
#     # update_hb_channel_ticket_weekly()
#
#     #his
#     import datetime
#     # end_date = datetime.date(2012, 11, 22)
#     # start_date = datetime.date(2017, 2, 6)
#     # while start_date > end_date:
#     #     start_week, end_week = DateUtil.get_this_week_date(end_date)
#     #     update_hb_channel_ticket_weekly(start_week, end_week)
#     #     # print start_week, end_week
#     #     end_date = end_week
#
#     end_date = datetime.date(2012, 11, 22)
#     start_date = datetime.date(2017, 2, 6)
#     while start_date > end_date:
#         start_week, end_week = DateUtil.get_this_week_date(end_date)
#         update_hb_company_ticket_weekly(start_week, end_week)
#         # print start_week, end_week
#         end_date = end_week
#     # update_hb_company_ticket_weekly()