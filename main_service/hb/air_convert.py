# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def airport_info_covert_hourly():
    today = DateUtil.get_date_after_days(0)
    max_id = DBCli().airport_flight_cli.query_one("select max(id) from airport_statistics")[0]
    sql = """
        select id, airport_code, local_date, start, end,
        replace(json_extract(data, '$.airportState'), '"', '') as airportState,
        ifnull(replace(json_extract(data, '$.airportPlanIn'), '"', ''), 0) as airportPlanIn,
        ifnull(replace(json_extract(data, '$.airportDelayIn'), '"', ''), 0) as airportDelayIn,
        ifnull(replace(json_extract(data, '$.airportPlanOut'), '"', ''), 0) as airportPlanOut,
        ifnull(replace(json_extract(data, '$.airportTotalIn'), '"', ''), 0) as airportTotalIn,
        ifnull(replace(json_extract(data, '$.airportTotalOut'), '"', ''), 0) as airportTotalOut,
        ifnull(replace(json_extract(data, '$.airportDelayOut'), '"', ''), 0) as airportDelayOut,
        ifnull(replace(json_extract(data, '$.airportStateDelayOut'), '"', ''), 0) as airportStateDelayOut,
        ifnull(replace(json_extract(data, '$.airportCancelOut'), '"', ''), 0) as airportCancelOut,
        ifnull(replace(json_extract(data, '$.airportAlter'), '"', ''), 0) as airportAlter,
        ifnull(replace(json_extract(data, '$.airportReturn'), '"', ''), 0) as airportReturn
        from airport_statistics
    """

    insert_sql = """
        insert into airport_statistics (id, airport_code, local_date, start, end, airportState,
        airportPlanIn, airportDelayIn, airportPlanOut, airportTotalIn, airportTotalOut, airportDelayOut,
        airportStateDelayOut, airportCancelOut, airportAlter, airportReturn, create_time, update_time)
        values
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update update_time = now(),
        id= values(id),
        airport_code= values(airport_code),
        local_date=values(local_date),
        start = values(start),
        end=values(end),
        airportState=values(airportState),
        airportPlanIn=values(airportPlanIn),
        airportDelayIn=values(airportDelayIn),
        airportPlanOut=values(airportPlanOut),
        airportTotalIn=values(airportTotalIn),
        airportTotalOut=values(airportTotalOut),
        airportDelayOut=values(airportDelayOut),
        airportStateDelayOut=values(airportStateDelayOut),
        airportCancelOut=values(airportCancelOut),
        airportAlter=values(airportAlter),
        airportReturn=values(airportReturn)
    """
    result = DBCli().airport_flight_cli.query_all(sql, [today, max_id])
    DBCli().targetdb_cli.batch_insert(insert_sql, result)


if __name__ == '__main__':
    airport_info_covert_hourly()