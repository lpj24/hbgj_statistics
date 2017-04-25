# -*- coding: utf-8 -*-
from sql.gt_sqlHandlers import gt_order_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_gt_order_daily(days=0):
    """更新高铁订单(日), gtgj_order_daily"""
    dto = []
    if days > 0:
        today = DateUtil.date2str(DateUtil.get_date_before_days(3))
        tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(0))
        for i in xrange(0, 4):
            dto.append(today)
            dto.append(tomorrow)
        query_data = DBCli().gt_cli.queryAll(gt_order_sql["gtgj_order_daily_his"], dto)
    else:
        today = DateUtil.date2str(DateUtil.get_date_before_days(days))
        tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(1))
        for i in xrange(0, 6):
            dto.append(today)
            dto.append(tomorrow)
        query_data = DBCli().gt_cli.queryAll(gt_order_sql["gtgj_order_daily"], dto)

    DBCli().targetdb_cli.batchInsert(gt_order_sql["update_gtgj_order_daily"], query_data)
    return __file__


def update_gt_order_hourly(days=0):
    today = DateUtil.date2str(DateUtil.get_date_before_days(days))
    tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(1-int(days)))
    dto = [today, tomorrow]
    query_data = DBCli().gt_cli.queryAll(gt_order_sql["gtgj_order_hourly"], dto)
    for hour_data in query_data:
        tmp_dto = [hour_data[0], hour_data[1]]
        q_hour_data = DBCli().targetdb_cli.queryOne(gt_order_sql["query_gtgj_order_by_hour"], tmp_dto)
        if q_hour_data:
            DBCli().targetdb_cli.insert(gt_order_sql["update_gtgj_order_hourly"], hour_data[2:]+hour_data[0:2])
        else:
            DBCli().targetdb_cli.insert(gt_order_sql["insert_gtgj_order_hourly"], hour_data)


if __name__ == "__main__":
    update_gt_order_daily(1)
    # update_gt_order_hourly()
