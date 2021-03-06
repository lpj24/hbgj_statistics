# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_his():
    sql = """
        select sday, partner, pv, uv from flight_partnerapi_srv_day
    """

    insert_sql = """
        insert into hbdt_flight_partnerapi (s_day, partner, pv, uv, createtime, updatetime) values
        (%s, %s, %s, %s, now(), now())
    """

    query_data = DBCli().hb_partner_cli.query_all(sql)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)


def update_hb_partner_daily(days=0):
    """航班合作伙伴pv和uv, hbdt_flight_partnerapi"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 5), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - days), '%Y-%m-%d')

    sql = """
        select sday, partner, pv, uv from flight_partnerapi_srv_day where sday >= %s
        and sday < %s
    """

    insert_sql = """
        insert into hbdt_flight_partnerapi (s_day, partner, pv, uv, createtime, updatetime)
        values (%s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        partner = VALUES(partner),
        pv = VALUES(pv),
        uv = VALUES(uv)
    """
    query_data = DBCli().dynamic_focus_cli.query_all(sql, [start_date, end_date])
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)


if __name__ == "__main__":
    # update_his()
    update_hb_partner_daily(1)
    # from tornado import gen
    # 
    # 
    # @gen.coroutine
    # def fetch_coroutine(url):
    #     http_client = AsyncHTTPClient()
    #     response = yield http_client.fetch(url)
    #     raise gen.Return(response.body)

    from tornado import gen


    # @gen.coroutine
    # def fetch_coroutine(url):
    #     http_client = AsyncHTTPClient()
    #     response = yield http_client.fetch(url)
    #     raise gen.Return(response.body)
