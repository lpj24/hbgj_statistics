# -*- coding: utf-8 -*-
from monitor_sql import sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import operator


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
    column_info_data = DBCli().targetdb_cli.queryAll(info_sql, [table_name])
    cal_column = []

    l_day = DateUtil.date2str(DateUtil.get_date_before_days(1), '%Y-%m-%d')
    n_day = DateUtil.date2str(DateUtil.get_date_before_days(2), '%Y-%m-%d')

    pri_column, pri, pri_type = column_info_data[0]

    if pri_column == "s_day" and pri == "PRI":

        for column_info in column_info_data:
            column_n, column_key, data_type = column_info
            if data_type == "int" or data_type == "decimal":
                cal_column.append([column_n, table_name, l_day, column_n, table_name, n_day])
    # s_day不是主键
    else:
        # TODO
        pass
    return cal_column


def cal_balance():
    all_table = get_all_monitor_table_name()
    exception_table = []

    for tab_name in all_table:

        require_query_column = get_table_column_info(tab_name)
        for require_query_dto in require_query_column:
            s_day_sql = "select {} from {} where s_day=%s union select {} from {} where s_day=%s"
            column, table_name, last_day, column_name, table_name, next_day = require_query_dto
            s_day_sql = s_day_sql.format(column, table_name, column_name, table_name)
            dto = [last_day, next_day]
            require_compare_data = DBCli().targetdb_cli.queryAll(s_day_sql, dto)
            # 差额相差30%
            try:
                last_data = float(require_compare_data[0][0])
                next_data = float(require_compare_data[1][0])
                if len(str(next_data)) <= 4:
                    continue
                balance_num_boolean = True if last_data == 0 else float(operator.abs(last_data - next_data)) / float(last_data) > 0.7
            except (Exception, ZeroDivisionError):
                break

            if balance_num_boolean:
                exception_table.append(table_name)
    return set(exception_table)


if __name__ == "__main__":
    print cal_balance()
#
#     all_table = get_all_monitor_table_name()
#
#     for tab_name in all_table:
#         require_query_column = get_table_column_info(tab_name)




