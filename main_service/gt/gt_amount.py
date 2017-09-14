# -*- coding: utf-8 -*-
from sql.gt_sqlHandlers import gt_amount_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def getGtAmountSuccess(dto, days):
    if days > 0:
        success_sql = gt_amount_sql["gtgj_amount_success"]
        amount_success = DBCli().gt_cli.queryAll(success_sql, dto)
    else:
        success_sql = gt_amount_sql["gtgj_amount_success_daily"]
        amount_success = DBCli().targetdb_cli.queryAll(success_sql, dto)
    return list(amount_success)


def getGtAmountCreate(dto):
    amount_create = DBCli().targetdb_cli.queryAll(gt_amount_sql["gtgj_amount_create"], dto)
    return list(amount_create)


def getGtAmountgrab(dto):
    amount_grab = DBCli().targetdb_cli.queryAll(gt_amount_sql["gtgj_amount_grab"], dto)
    return list(amount_grab)


def getGtAmountChange(start_date, end_date):
    change_amount = 0
    dto = [start_date, end_date]
    query_ids = DBCli().gt_cli.queryAll(gt_amount_sql["gtgj_change_oids"], dto)
    for oid in query_ids:
        change_order_info = DBCli().gt_cli.queryAll(gt_amount_sql["gtgj_change_info"], [oid])
        info_list = []
        for order_info in change_order_info:
            order_info = list(order_info)
            order_info[1] = ((order_info[1] is None) and [None] or [DateUtil.date2str(order_info[1])])[0]
            order_info[4] = ((order_info[4] is None) and [None] or [DateUtil.date2str(order_info[4])])[0]
            info_list.append(order_info)

        timeMap = {}
        for strs in info_list:
            if strs[3] == u"改签票" or (strs[3] == u"已退票" and (strs[1] is not None) and (strs[1] != strs[4])):
                timeList = timeMap.get(strs[1])
                if timeList is None:
                    timeList = []
                    timeMap[strs[1]] = timeList
                timeList.append(strs[0])

        for index in timeMap.values():
            newprice = 0
            oldpriice = 0
            for strs in info_list:
                if index.count(strs[0]) > 0:
                    if strs[3] == u"改签票" or (strs[3] == u"已退票" and (strs[1] is not None) and (strs[1] != strs[4])):

                        newprice += float(strs[2])
                    else:
                        oldpriice += float(strs[2])

            if oldpriice < newprice:
                change_amount += newprice

    return int(round(change_amount))


def update_gtgj_amount_daily(days=0):
    """更新高铁交易额(日), gtgj_amount_daily"""
    if days > 0:
        query_date = DateUtil.get_date_before_days(3)
        today = DateUtil.get_date_before_days(3)
        tomorrow = DateUtil.get_date_after_days(0)
    else:
        query_date = DateUtil.get_date_before_days(days)
        today = DateUtil.get_date_before_days(days)
        tomorrow = DateUtil.get_date_after_days(1)
    dto = [DateUtil.date2str(today, '%Y-%m-%d'), DateUtil.date2str(tomorrow, '%Y-%m-%d')]
    amount_sucess = getGtAmountSuccess(dto, days)
    amount_create = getGtAmountCreate(dto)
    amount_grab = getGtAmountgrab(dto)

    amount_change_list = []

    while today < tomorrow:
        end_date = DateUtil.add_days(today, 1)
        amount_change = getGtAmountChange(DateUtil.date2str(today), DateUtil.date2str(end_date))
        amount_change = (amount_change,)
        amount_change_list.append(amount_change)
        today = DateUtil.add_days(today, 1)

    amount_sucess.reverse()
    amount_create.reverse()
    amount_grab.reverse()
    amount_change_list.reverse()

    for i in xrange(len(amount_create)):
        query_data = []
        query_data.append(DateUtil.date2str(query_date, '%Y-%m-%d'))
        query_data.append(int_round(amount_sucess.pop()[0]))
        query_data.append(int_round(amount_change_list.pop()[0]))
        query_data.append(int_round(amount_create.pop()[0]))
        query_data.append(int_round(amount_grab.pop()[0]))
        query_date = DateUtil.add_days(query_date, 1)
        DBCli().targetdb_cli.insert(gt_amount_sql["update_gtgj_amount_daily"], query_data)
    pass


def int_round(num):
    return int(round(num))


if __name__ == "__main__":
    update_gtgj_amount_daily(1)