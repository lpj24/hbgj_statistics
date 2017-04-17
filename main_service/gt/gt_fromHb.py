# -*- coding: utf-8 -*-
from sql.gt_sqlHandlers import gt_order_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_gtgj_from_hb(days=0):
    """更新由航班转换到高铁的订单, gtgj_ticket_from_hb"""
    today = DateUtil.date2str(DateUtil.get_date_before_days(int(days)))
    tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)))
    dto = [today, tomorrow]
    query_data_order = DBCli().gt_cli.queryOne(gt_order_sql["gtgj_order_from_hb"], dto)
    query_data_ticket = DBCli().gt_cli.queryOne(gt_order_sql["gtgj_ticket_from_hb"], dto)

    query_data = [query_data_order[0], query_data_order[1], query_data_ticket[1]]
    DBCli().targetdb_cli.insert(gt_order_sql["insert_gtgj_from_hb"], query_data)
    return __file__


# def update_gtgj_from_hb_his():
#     start_date = datetime.date(2016, 6, 1)
#     end_date = datetime.date(2016, 12, 27)
#     while start_date < end_date:
#         query_end = DateUtil.add_days(start_date, 1)
#         dto = [DateUtil.date2str(start_date), DateUtil.date2str(query_end)]
#         query_data_order = DBCli().gt_cli.queryOne(gt_order_sql["gtgj_order_from_hb"], dto)
#         query_data_ticket = DBCli().gt_cli.queryOne(gt_order_sql["gtgj_ticket_from_hb"], dto)
#
#         order_num = query_data_order[1] if query_data_order else 0
#         ticket_num = query_data_ticket[1] if query_data_ticket else 0
#
#         query_data = [DateUtil.date2str(start_date, '%Y-%m-%d'), order_num, ticket_num]
#         DBCli().targetdb_cli.insert(gt_order_sql["insert_gtgj_from_hb"], query_data)
#         start_date = DateUtil.add_days(start_date, 1)

if __name__ == "__main__":
    update_gtgj_from_hb(2)
    # update_gtgj_from_hb_his()