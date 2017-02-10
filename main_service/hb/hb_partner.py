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

    query_data = DBCli().hb_partner_cli.queryAll(sql)
    DBCli().targetdb_cli.batchInsert(insert_sql, query_data)


def update_hb_partner_daily(days=0):
    s_day = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    sql = """
        select sday, partner, pv, uv from flight_partnerapi_srv_day where sday=%s
    """
    query_data = DBCli().hb_partner_cli.queryAll(sql, [s_day])
    DBCli().targetdb_cli.batchInsert(sql, query_data)

if __name__ == "__main__":
    update_his()