# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_huoli_car_orders_daily(days=0):
    """更新专车订单(新), huolicar_orders_daily"""
    start_date = DateUtil.get_date_before_days(int(days) * 1344)
    end_date = DateUtil.get_date_after_days(1-int(days))
    order_sql = """
            SELECT
            t0.date,
            COUNT(DISTINCT CASE
                WHEN `订单类型` = '接送机' THEN orderid
                ELSE 0
            END) AS '接送机',
                COUNT(DISTINCT CASE
                WHEN `订单类型` = '接送机' and new_paramp like '%%ios%%' THEN orderid
                ELSE 0
            END) AS '接送机_ios',
                COUNT(DISTINCT CASE
                WHEN `订单类型` = '接送机' and new_paramp like '%%android%%' THEN orderid
                ELSE 0
            END) AS '接送机_android',
                COUNT(DISTINCT CASE
                WHEN `订单类型` = '接送机' and new_paramp not like '%%android%%' and new_paramp not like '%%ios%%' THEN orderid
                ELSE 0
            END) AS '接送机_else',

            COUNT(DISTINCT CASE
                WHEN `订单类型` = '接送站' THEN orderid
                ELSE 0
            END) AS '接送站',
                COUNT(DISTINCT CASE
                WHEN `订单类型` = '接送站' and new_paramp like '%%ios%%' THEN orderid
                ELSE 0
            END) AS '接送站_ios',
                COUNT(DISTINCT CASE
                WHEN `订单类型` = '接送站' and new_paramp like '%%android%%' THEN orderid
                ELSE 0
            END) AS '接送站_android',
                COUNT(DISTINCT CASE
                WHEN `订单类型` = '接送站' and new_paramp not like '%%android%%' and new_paramp not like '%%ios%%' THEN orderid
                ELSE 0
            END) AS '接送站_else',

            COUNT(DISTINCT CASE
                WHEN `订单类型` = '拼车' THEN orderid
                ELSE 0
            END) AS '拼车',
                COUNT(DISTINCT CASE
                WHEN `订单类型` = '拼车' and new_paramp like '%%ios%%' THEN orderid
                ELSE 0
            END) AS '拼车_ios',
                COUNT(DISTINCT CASE
                WHEN `订单类型` = '拼车' and new_paramp like '%%android%%' THEN orderid
                ELSE 0
            END) AS '拼车_android',
                COUNT(DISTINCT CASE
                WHEN `订单类型` = '拼车' and new_paramp not like '%%android%%' and new_paramp not like '%%ios%%' THEN orderid
                ELSE 0
            END) AS '拼车_else',

            COUNT(orderid) '总量',
                COUNT(DISTINCT CASE when new_paramp like '%%ios%%' then orderid end) as '总量_ios',
                COUNT(DISTINCT CASE when new_paramp like '%%android%%' then orderid end) as '总量_android',
                COUNT(DISTINCT CASE when new_paramp not like '%%android%%' and orderid not like '%%ios%%' then phone_id end) as '总量_android',

                COUNT(DISTINCT CASE
                WHEN `订单类型` = '其他' THEN orderid
                ELSE 0
            END) AS '其他'
        FROM
            (SELECT
                DATE(t1.createtime) AS 'date',
                    t1.phone_id AS phone_id,
                    t2.paramp as new_paramp,
                    t2.order_id as orderid,
                    CASE
                        WHEN t2.get_type IN (1 , 2) THEN '接送机'
                        WHEN t2.get_type IN (5 , 6) THEN '接送站'
                        WHEN t2.get_type IN (31 , 32, 35, 36) THEN '拼车'
                        ELSE '其他'
                    END AS '订单类型'
            FROM
                car_user t1
            INNER JOIN orders t2 ON (t1.phone_id = t2.phone_id
                AND t1.first_time = t2.createtime)
            INNER JOIN order_balance t3 ON t2.order_id = t3.order_id
            WHERE
                t1.createtime >= %s
                    AND t1.createtime <= %s
            ) t0
        GROUP BY t0.date


    """

    insert_sql = """
        insert into huolicar_orders_daily values (%s, %s, %s, %s, %s, %s, %s, %s,%s, %s,
        %s,%s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        orders_num_jsj = VALUES(orders_num_jsj),
        orders_num_ios_jsj = VALUES(orders_num_ios_jsj),
        orders_num_android_jsj = VALUES(orders_num_android_jsj),
        orders_num_else_jsj = VALUES(orders_num_else_jsj),
        orders_num_jsz = VALUES(orders_num_jsz),
        orders_num_ios_jsz = VALUES(orders_num_ios_jsz),
        orders_num_android_jsz = VALUES(orders_num_android_jsz),
        orders_num_else_jsz = VALUES(orders_num_else_jsz),
        orders_num_pc = VALUES(orders_num_pc),
        orders_num_ios_pc = VALUES(orders_num_ios_pc),
        orders_num_android_pc = VALUES(orders_num_android_pc),
        orders_num_ios_pc = VALUES(orders_num_ios_pc),
        orders_num = VALUES(orders_num),
        orders_num_ios = VALUES(orders_num_ios),
        orders_num_android = VALUES(orders_num_android),
        orders_num_else = VALUES(orders_num_else),
        orders_else = VALUES(orders_else)
    """
    dto = [start_date, end_date]
    query_data = DBCli().car_cli.query_all(order_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)


def update_huoli_car_consumers_daily(days=0):
    """更新专车订单(新), huolicar_consumers_daily"""
    start_date = DateUtil.get_date_before_days(int(days) * 1)
    end_date = DateUtil.get_date_after_days(1-int(days))
    order_sql = """
            SELECT
            t0.date,
            SUM(CASE
                WHEN `订单类型` = '接送机' THEN 1
                ELSE 0
            END) AS '接送机',
                SUM(CASE
                WHEN `订单类型` = '接送机' and new_paramp like '%%ios%%' THEN 1
                ELSE 0
            END) AS '接送机_ios',
                SUM(CASE
                WHEN `订单类型` = '接送机' and new_paramp like '%%android%%' THEN 1
                ELSE 0
            END) AS '接送机_android',
                SUM(CASE
                WHEN `订单类型` = '接送机' and new_paramp not like '%%android%%' and new_paramp not like '%%ios%%' THEN 1
                ELSE 0
            END) AS '接送机_else',



            SUM(CASE
                WHEN `订单类型` = '接送站' THEN 1
                ELSE 0
            END) AS '接送站',
                SUM(CASE
                WHEN `订单类型` = '接送站' and new_paramp like '%%ios%%' THEN 1
                ELSE 0
            END) AS '接送站_ios',
                SUM(CASE
                WHEN `订单类型` = '接送站' and new_paramp like '%%android%%' THEN 1
                ELSE 0
            END) AS '接送站_android',
                SUM(CASE
                WHEN `订单类型` = '接送站' and new_paramp not like '%%android%%' and new_paramp not like '%%ios%%' THEN 1
                ELSE 0
            END) AS '接送站_else',


            SUM(CASE
                WHEN `订单类型` = '拼车' THEN 1
                ELSE 0
            END) AS '拼车',
                SUM(CASE
                WHEN `订单类型` = '拼车' and new_paramp like '%%ios%%' THEN 1
                ELSE 0
            END) AS '拼车_ios',
                SUM(CASE
                WHEN `订单类型` = '拼车' and new_paramp like '%%android%%' THEN 1
                ELSE 0
            END) AS '拼车_android',
                SUM(CASE
                WHEN `订单类型` = '拼车' and new_paramp not like '%%android%%' and new_paramp not like '%%ios%%' THEN 1
                ELSE 0
            END) AS '拼车_else',


            SUM(CASE
                WHEN `订单类型` = '其他' THEN 1
                ELSE 0
            END) AS '其他',
            COUNT(phone_id) '总量',
                count(case when new_paramp like '%%ios%%' then phone_id end) as '总量_ios',
                count(case when new_paramp like '%%android%%' then phone_id end) as '总量_android',
                count(case when new_paramp not like '%%android%%' and new_paramp not like '%%ios%%' then phone_id END) '总量_android'
        FROM
            (SELECT
                DATE(t1.createtime) AS 'date',
                    t1.phone_id AS phone_id,
                                t2.paramp as new_paramp,
                    CASE
                        WHEN t2.get_type IN (1 , 2) THEN '接送机'
                        WHEN t2.get_type IN (5 , 6) THEN '接送站'
                        WHEN t2.get_type IN (31 , 32, 35, 36) THEN '拼车'
                        ELSE '其他'
                    END AS '订单类型'
            FROM
                car_user t1
            INNER JOIN orders t2 ON (t1.phone_id = t2.phone_id
                AND t1.first_time = t2.createtime)
            INNER JOIN order_balance t3 ON t2.order_id = t3.order_id
            WHERE
                t1.createtime >= %s
                    AND t1.createtime <= %s
            GROUP BY t1.phone_id) t0
        GROUP BY t0.date

    """
    insert_sql = """
        insert into huolicar_consumers_daily values (%s, %s, %s, %s, %s, %s, %s, %s,%s, %s,
        %s,%s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        consumers_num_jsj = VALUES(consumers_num_jsj),
        consumers_num_ios_jsj = VALUES(consumers_num_ios_jsj),
        consumers_num_android_jsj = VALUES(consumers_num_android_jsj),
        consumers_num_else_jsj = VALUES(consumers_num_else_jsj),
        consumers_num_jsz = VALUES(consumers_num_jsz),
        consumers_num_ios_jsz = VALUES(consumers_num_ios_jsz),
        consumers_num_android_jsz = VALUES(consumers_num_android_jsz),
        consumers_num_else_jsz = VALUES(consumers_num_else_jsz),
        consumers_num_pc = VALUES(consumers_num_pc),
        consumers_num_ios_pc = VALUES(consumers_num_ios_pc),
        consumers_num_android_pc = VALUES(consumers_num_android_pc),
        consumers_num_ios_pc = VALUES(consumers_num_ios_pc),
        consumers_num = VALUES(consumers_num),
        consumers_num_ios = VALUES(consumers_num_ios),
        consumers_num_android = VALUES(consumers_num_android),
        consumers_num_else = VALUES(consumers_num_else),
        consumers_else = VALUES(consumers_else)
    """
    dto = [start_date, end_date]
    query_data = DBCli().car_cli.query_all(order_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)


def update_huoli_car_newconsumers_daily(days=0):
    """更新专车订单(新), huolicar_newconsumers_daily"""
    start_date = DateUtil.get_date_before_days(int(days) * 1)
    end_date = DateUtil.get_date_after_days(1-int(days))
    order_sql = """
            SELECT
            t0.date,
            SUM(CASE
                WHEN `订单类型` = '接送机' THEN 1
                ELSE 0
            END) AS '接送机',
                SUM(CASE
                WHEN `订单类型` = '接送机' and new_paramp like '%%ios%%' THEN 1
                ELSE 0
            END) AS '接送机_ios',
                SUM(CASE
                WHEN `订单类型` = '接送机' and new_paramp like '%%android%%' THEN 1
                ELSE 0
            END) AS '接送机_android',
                SUM(CASE
                WHEN `订单类型` = '接送机' and new_paramp not like '%%android%%' and new_paramp not like '%%ios%%' THEN 1
                ELSE 0
            END) AS '接送机_else',



            SUM(CASE
                WHEN `订单类型` = '接送站' THEN 1
                ELSE 0
            END) AS '接送站',
                SUM(CASE
                WHEN `订单类型` = '接送站' and new_paramp like '%%ios%%' THEN 1
                ELSE 0
            END) AS '接送站_ios',
                SUM(CASE
                WHEN `订单类型` = '接送站' and new_paramp like '%%android%%' THEN 1
                ELSE 0
            END) AS '接送站_android',
                SUM(CASE
                WHEN `订单类型` = '接送站' and new_paramp not like '%%android%%' and new_paramp not like '%%ios%%' THEN 1
                ELSE 0
            END) AS '接送站_else',


            SUM(CASE
                WHEN `订单类型` = '拼车' THEN 1
                ELSE 0
            END) AS '拼车',
                SUM(CASE
                WHEN `订单类型` = '拼车' and new_paramp like '%%ios%%' THEN 1
                ELSE 0
            END) AS '拼车_ios',
                SUM(CASE
                WHEN `订单类型` = '拼车' and new_paramp like '%%android%%' THEN 1
                ELSE 0
            END) AS '拼车_android',
                SUM(CASE
                WHEN `订单类型` = '拼车' and new_paramp not like '%%android%%' and new_paramp not like '%%ios%%' THEN 1
                ELSE 0
            END) AS '拼车_else',


            SUM(CASE
                WHEN `订单类型` = '其他' THEN 1
                ELSE 0
            END) AS '其他',
            COUNT(phone_id) '总量',
                count(case when new_paramp like '%%ios%%' then phone_id end) as '总量_ios',
                count(case when new_paramp like '%%android%%' then phone_id end) as '总量_android',
                count(case when new_paramp not like '%%android%%' and new_paramp not like '%%ios%%' then phone_id END) '总量_android'
        FROM
            (SELECT
                DATE(t1.createtime) AS 'date',
                    t1.phone_id AS phone_id,
                                t2.paramp as new_paramp,
                    CASE
                        WHEN t2.get_type IN (1 , 2) THEN '接送机'
                        WHEN t2.get_type IN (5 , 6) THEN '接送站'
                        WHEN t2.get_type IN (31 , 32, 35, 36) THEN '拼车'
                        ELSE '其他'
                    END AS '订单类型'
            FROM
                car_user t1
            INNER JOIN orders t2 ON (t1.phone_id = t2.phone_id
                AND t1.first_time = t2.createtime)
            INNER JOIN order_balance t3 ON t2.order_id = t3.order_id
            WHERE
                t1.createtime >= %s
                    AND t1.createtime <= %s
            GROUP BY t1.phone_id) t0
        GROUP BY t0.date

    """
    insert_sql = """
        insert into huolicar_consumers_daily values (%s, %s, %s, %s, %s, %s, %s, %s,%s, %s,
        %s,%s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = VALUES(s_day),
        consumers_num_jsj = VALUES(consumers_num_jsj),
        consumers_num_ios_jsj = VALUES(consumers_num_ios_jsj),
        consumers_num_android_jsj = VALUES(consumers_num_android_jsj),
        consumers_num_else_jsj = VALUES(consumers_num_else_jsj),
        consumers_num_jsz = VALUES(consumers_num_jsz),
        consumers_num_ios_jsz = VALUES(consumers_num_ios_jsz),
        consumers_num_android_jsz = VALUES(consumers_num_android_jsz),
        consumers_num_else_jsz = VALUES(consumers_num_else_jsz),
        consumers_num_pc = VALUES(consumers_num_pc),
        consumers_num_ios_pc = VALUES(consumers_num_ios_pc),
        consumers_num_android_pc = VALUES(consumers_num_android_pc),
        consumers_num_ios_pc = VALUES(consumers_num_ios_pc),
        consumers_num = VALUES(consumers_num),
        consumers_num_ios = VALUES(consumers_num_ios),
        consumers_num_android = VALUES(consumers_num_android),
        consumers_num_else = VALUES(consumers_num_else),
        consumers_else = VALUES(consumers_else)
    """
    dto = [start_date, end_date]
    query_data = DBCli().car_cli.query_all(order_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, query_data)

if __name__ == '__main__':
    update_huoli_car_orders_daily(1)
    # update_huoli_car_consumers_daily(10)