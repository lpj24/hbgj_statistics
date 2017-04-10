# -*- coding: utf-8 -*-
from monitor_sql import sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def get_all_monitor_table_name():
    all_monitor_table = []
    for sql_str in sql:
        monitor_table = sql_str.split(" ")[3]
        all_monitor_table.append(monitor_table)

    return all_monitor_table


def get_table_column_info(table_name):
    info_sql = """
        SELECT column_name,column_key,data_type FROM information_schema.columns
        WHERE table_name=%s AND table_schema = "bi" and
        column_name != 'updatetime' and column_name != 'createtime';
    """

    s_day_sql = """
        select %s from %s where s_day=%s
        union
        select %s from %s where s_day=%s
    """
    column_info_data = DBCli().targetdb_cli.queryAll(info_sql, [table_name])
    cal_column = []

    pri_column, pri, pri_type = column_info_data[0]

    if pri_column == "s_day" and pri == "PRI":

        for column_info in column_info_data:
            column_name, column_key, data_type = column_info
            if data_type == "int" or data_type == "decimal":
                cal_column.append([column_name, table_name])
    return cal_column

if __name__ == "__main__":
    all_table = get_all_monitor_table_name()
    print get_table_column_info(all_table[0])
