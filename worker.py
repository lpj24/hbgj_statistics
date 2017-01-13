# from rq import Connection, Queue
# from redis import Redis
# import time
# from job import count_words_at_url
#
#
# if __name__ == "__main__":
#     redis_conn = Redis()
#     q = Queue(connection=redis_conn)
#     jobs = q.enqueue(count_words_at_url, "http://nvie.com")
#     print jobs.result
#     time.sleep(2)
#     print jobs.result

# import click
#
#
# @click.command()
# @click.option('--name', prompt='Your name', help='The person to greet.')
# @click.option('--gender', default='debug', type=click.Choice(["debug", "product"]), help='chose environment')
# def hello(name, gender):
#     click.secho("Hello %s, %s " % (name, gender), fg='red')
#
# if __name__ == "__main__":
#     hello()

if __name__ == "__main__":
    # sql = """
    #     select distinct TRADE_TIME s_day,
    #     sum(case when (AMOUNT_TYPE=2 and PRODUCT='0' and TRADE_CHANNEL not like '%coupon%') then amount else 0 end) paycost_in,
    #     sum(case when (AMOUNT_TYPE=3 and PRODUCT='0' and TRADE_CHANNEL not like '%coupon%') then amount else 0 end) paycost_return,
    #     sum(case when (AMOUNT_TYPE=1 and PRODUCT='0' and TRADE_CHANNEL like '%coupon%') then amount else 0 end) coupon_in,
    #     sum(case when (AMOUNT_TYPE=4 and PRODUCT='0' and TRADE_CHANNEL like '%coupon%') then amount else 0 end) coupon_return,
    #     sum(case when (AMOUNT_TYPE=6 and PRODUCT='20') then amount else 0 end) delay_care,
    #     sum(case when (AMOUNT_TYPE=5 and PRODUCT in ('1','2','3','4','9','10','12','14')) then amount else 0 end) point_give_amount,
    #     sum(case when (AMOUNT_TYPE=6 and PRODUCT in ('6','8','24','25')) then amount else 0 end) balance_give_amount
    #     from pay_cost_info
    #     group by TRADE_TIME
    # """
    #
    # from dbClient.db_client import DBCli
    # result = DBCli().pay_cost_cli.queryOne(sql)
    # insert_sql = """
    #     insert into profit_hb_cost (s_day, paycost_in, paycost_return, coupon_in, coupon_return,
    #     delay_care, point_give_amount, balance_give_amount, createtime, updatetime) values (
    #         %s, %s, %s, %s, %s, %s, %s, %s, now(), now()
    #     )
    # """
    # DBCli().targetdb_cli.batchInsert(insert_sql, result)
    from dbClient.db_client import DBCli
    car_sql = """
        select distinct TRADE_TIME s_day,
        sum(case when (AMOUNT_TYPE=2 and PRODUCT='7' and TRADE_CHANNEL not like '%coupon%') then amount else 0 end) paycost_in,
        sum(case when (AMOUNT_TYPE=3 and PRODUCT='7' and TRADE_CHANNEL not like '%coupon%') then amount else 0 end) paycost_return,
        sum(case when (AMOUNT_TYPE=1 and PRODUCT='7' and TRADE_CHANNEL like '%coupon%') then amount else 0 end) coupon_in,
        sum(case when (AMOUNT_TYPE=4 and PRODUCT='7' and TRADE_CHANNEL like '%coupon%') then amount else 0 end) coupon_return,
        sum(case when (AMOUNT_TYPE=5 and PRODUCT in ('5','13')) then amount else 0 end) point_give_amount,
        sum(case when (AMOUNT_TYPE=6 and PRODUCT in ('12','29')) then amount else 0 end) balance_give_amount
        from pay_cost_info
        group by s_day
    """

    result = DBCli().pay_cost_cli.queryAll(car_sql)
    insert_car_sql = """
        insert into profit_huoli_car_cost (s_day, paycost_in, paycost_return, coupon_in, coupon_return,
         point_give_amount, balance_give_amount, createtime, updatetime) values (
            %s, %s, %s, %s, %s, %s, %s, now(), now()
        )
    """
    DBCli().targetdb_cli.batchInsert(insert_car_sql, result)

    hotel_sql = """
        select distinct TRADE_TIME s_day,
        sum(case when (AMOUNT_TYPE=2 and PRODUCT='36' and TRADE_CHANNEL not like '%coupon%') then amount else 0 end) paycost_in,
        sum(case when (AMOUNT_TYPE=3 and PRODUCT='36' and TRADE_CHANNEL not like '%coupon%') then amount else 0 end) paycost_return,
        sum(case when (AMOUNT_TYPE=1 and PRODUCT='36' and TRADE_CHANNEL like '%coupon%') then amount else 0 end) coupon_in,
        sum(case when (AMOUNT_TYPE=4 and PRODUCT='36' and TRADE_CHANNEL like '%coupon%') then amount else 0 end) coupon_return,
        sum(case when (AMOUNT_TYPE=5 and PRODUCT in ('8')) then amount else 0 end) point_give_amount,
        sum(case when (AMOUNT_TYPE=6 and PRODUCT in ('9','10')) then amount else 0 end) balance_give_amount
        from pay_cost_info
        group by s_day
    """

    result = DBCli().pay_cost_cli.queryAll(hotel_sql)
    insert_hotel_sql = """
        insert into profit_huoli_hotel_cost (s_day, paycost_in, paycost_return, coupon_in, coupon_return,
         point_give_amount, balance_give_amount, createtime, updatetime) values (
            %s, %s, %s, %s, %s, %s, %s, now(), now()
        )
    """
    DBCli().targetdb_cli.batchInsert(insert_hotel_sql, result)
