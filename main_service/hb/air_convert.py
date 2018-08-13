# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
import os


def airport_info_covert_hourly():
    """更新机场延误情况每15钟更新一次, airport_statistics"""
    sql = """
        select id, airport_code, date_format(local_date, '%%Y-%%m-%%d') local_date, start, end,
        CAST(JSON_UNQUOTE(json_extract(data, '$.airportState')) AS CHAR) as airportState,
        ifnull(JSON_UNQUOTE(json_extract(data, '$.airportPlanIn')), 0) as airportPlanIn,
        ifnull(JSON_UNQUOTE(json_extract(data, '$.airportDelayIn')), 0) as airportDelayIn,
        ifnull(JSON_UNQUOTE(json_extract(data, '$.airportPlanOut')), 0) as airportPlanOut,
        ifnull(JSON_UNQUOTE(json_extract(data, '$.airportTotalIn')), 0) as airportTotalIn,
        ifnull(JSON_UNQUOTE(json_extract(data, '$.airportTotalOut')), 0) as airportTotalOut,
        ifnull(JSON_UNQUOTE(json_extract(data, '$.airportDelayOut')), 0) as airportDelayOut,
        ifnull(JSON_UNQUOTE(json_extract(data, '$.airportStateDelayOut')), 0) as airportStateDelayOut,
        ifnull(JSON_UNQUOTE(json_extract(data, '$.airportCancelIn')), 0) as airportCancelIn,
        ifnull(JSON_UNQUOTE(json_extract(data, '$.airportCancelOut')), 0) as airportCancelOut,
        ifnull(JSON_UNQUOTE(json_extract(data, '$.airportAlter')), 0) as airportAlter,
        ifnull(JSON_UNQUOTE(json_extract(data, '$.airportReturn')), 0) as airportReturn,
        now(), now()
        from airport_statistics where id>%s
    """

    insert_sql = """
        insert into airport_statistics (id, airport_code, local_date, start, end, airportState,
        airportPlanIn, airportDelayIn, airportPlanOut, airportTotalIn, airportTotalOut, airportDelayOut,
        airportStateDelayOut, airportCancelIn, airportCancelOut, airportAlter, airportReturn, create_time, update_time)
        values
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
    """

    max_target_db_id_sql = """
        select max(id) from airport_statistics
    """
    target_db_max_id = DBCli().targetdb_cli.query_one(max_target_db_id_sql)
    new_airport_result = DBCli().airport_flight_cli.query_all(sql, target_db_max_id[0])
    #
    with open('data.sql', 'wb') as f:
        for n_a in new_airport_result:
            f.write(','.join(map(unicode, n_a)) + '\n')
    DBCli().targetdb_cli.insert("load data local infile './data.sql' into table airport_statistics character set utf8 fields terminated by ',';")
    os.remove('./data.sql')


if __name__ == '__main__':
    airport_info_covert_hourly()