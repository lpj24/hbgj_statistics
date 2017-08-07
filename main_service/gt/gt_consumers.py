# -*- coding: utf-8 -*-
from sql.gt_sqlHandlers import gtgj_consumers_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from pybloom import BloomFilter
import os


def update_gtgj_consumers_daily(days=0):
    """更新高铁消费用户, gtgj_consumers_daily"""
    if days > 0:
        today = DateUtil.date2str(DateUtil.get_date_before_days(3))
        tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(0))
    else:
        today = DateUtil.date2str(DateUtil.get_date_before_days(days))
        tomorrow = DateUtil.date2str(DateUtil.get_date_after_days(1))
    dto = [today, tomorrow]
    query_data = DBCli().gt_cli.queryAll(gtgj_consumers_sql["gtgj_consumers_daily"], dto)
    DBCli().targetdb_cli.batchInsert(gtgj_consumers_sql["update_gtgj_consumers_daily"], query_data)
    return __file__


def update_gtgj_consumers_weekly():
    start_date = DateUtil.date2str(DateUtil.get_last_week_date(DateUtil.get_last_week_date()[0])[0])
    end_date = DateUtil.date2str(DateUtil.get_last_week_date()[1])
    dto = [start_date, end_date, start_date, end_date]
    query_data = DBCli().gt_cli.queryAll(gtgj_consumers_sql["gtgj_consumers_weekly"], dto)
    DBCli().targetdb_cli.batchInsert(gtgj_consumers_sql["update_gtgj_consumers_weekly"], query_data)


def update_gtgj_consumers_monthly():
    start_date, end_date = DateUtil.get_last_month_date()
    dto = [DateUtil.date2str(start_date, "%Y-%m-%d"), DateUtil.date2str(start_date), DateUtil.date2str(end_date),
           DateUtil.date2str(start_date), DateUtil.date2str(end_date)]
    query_data = DBCli().gt_cli.queryOne(gtgj_consumers_sql["gtgj_consumers_monthly"], dto)
    DBCli().targetdb_cli.insert(gtgj_consumers_sql["update_gtgj_consumers_monthly"], query_data)


def update_gtgj_consumers_quarterly():
    start_date, end_date = DateUtil.get_last_quarter_date()
    start_date = DateUtil.date2str(start_date)
    end_date = DateUtil.date2str(end_date)
    dto = [start_date, start_date, start_date, end_date, start_date, end_date]
    query_data = DBCli().gt_cli.queryOne(gtgj_consumers_sql["gtgj_consumers_quarterly"], dto)
    DBCli().targetdb_cli.insert(gtgj_consumers_sql["update_gtgj_consumers_quarterly"], query_data)


def storage_gt_consumers_quarter():
    sql = """
            select DISTINCT uid consumers
            from (
            select uid
            from user_order_history
            where i_status=3 and create_time>=%s
            and create_time<%s
            UNION
            select uid
            from user_order
            where i_status=3 and create_time>=%s
            and create_time<%s
            ) as A
    """
    start_date, end_date = DateUtil.get_last_quarter_date()
    start_date = DateUtil.date2str(start_date)
    end_date = DateUtil.date2str(end_date)
    # start_date = '2017-04-01'
    # end_date = '2017-07-01'
    dto = [start_date, end_date, start_date, end_date]
    query_data = DBCli().gt_cli.queryAll(sql, dto)
    g = BloomFilter(capacity=20000000, error_rate=0.00001)

    bloom_year_file = start_date + "_bloom_file.dat"
    for uid in query_data:
        uid = uid[0]
        # DBCli().redis_cli.sadd("gt_consumers", uid)
        g.add(uid)

    with open(os.path.join("/home/huolibi/data/hbdt/gt", bloom_year_file), "wb") as new_file:
        g.tofile(new_file)


def count_consumers():
    four_quarter = ['2017-01-01', '2017-04-01']
    bloom_obj = []
    for q in four_quarter:
        b_f = os.path.join("/home/huolibi/data/hbdt/gt", q + "_bloom_file.dat")
        with open(b_f, 'rb') as bloom_file:
            b_f = BloomFilter.fromfile(bloom_file)
            bloom_obj.append(b_f)
    # 6306532
    return reduce(lambda x, y: x.union(y), bloom_obj)


if __name__ == "__main__":
    #每小时更新当天数据  凌晨更新前三天
    # storage_gt_consumers_quarter()
    print len(count_consumers())
    # g = BloomFilter(capacity=20000000, error_rate=0.001)
    # with open("./bloom.txt", "rb") as bloom_f:
    #     gg = g.fromfile(bloom_f)
    # print len(gg)
    # update_gtgj_consumers_daily(1)

    # update_gtgj_consumers_weekly()
    # update_gtgj_consumers_quarterly()
    # import pandas as pd
    # from dbClient.db_client import DBCli
    # from xpinyin import Pinyin
    # p = Pinyin()
    #
    # def get_europe_air():
    #     europe_info = pd.read_excel("C:\\Users\\Administrator\\Desktop\\test.xlsm", sheetname="Sheet1")
    #     first_name = europe_info['M']
    #     last_name = europe_info['ING']
    #     first_name_list = [i for i in first_name]
    #     last_name_list = [j for j in last_name]
    #     # europe_info_code = europe_info["THREE_WORDS_CODE"]
    #     # europe_code_list = [e for e in europe_info_code]
    #     # return europe_code_list
    #     return first_name_list, last_name_list
    # f_first, l_last = get_europe_air()
    # # for i in f_first:
    # #     print p.get_pinyin(i, "")
    #
    # for j in l_last:
    #     if isinstance(j, unicode):
    #         print p.get_pinyin(j, "")
    #     else:
    #         print j
