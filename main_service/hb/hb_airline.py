# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_hbgj_client_airline_inland_weekly():
    """hbgj gtgj supply各航线数据, hbgj_client_airline_inland_weekly"""
    start_date, end_date = DateUtil.get_last_week_date()
    start_date = DateUtil.date2str(start_date, '%Y-%m-%d')
    end_date = DateUtil.date2str(end_date, '%Y-%m-%d')
    hbgj_sql = """
        SELECT %s, 'hbgj',
        DEPCODE,ARRCODE,
        SUM(case when INCOMETYPE =0 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_0,
        SUM(case when INCOMETYPE =0 then 1 else 0 end) ticket_count_0,
        SUM(case when INCOMETYPE =0 then od.PRICE else 0 end) amount_0,
        SUM(case when INCOMETYPE =1 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_1,
        SUM(case when INCOMETYPE =1 then 1 else 0 end) ticket_count_1,
        SUM(case when INCOMETYPE =1 then od.PRICE else 0 end) amount_1,
        SUM(case when INCOMETYPE =2 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_2,
        SUM(case when INCOMETYPE =2 then 1 else 0 end) ticket_count_2,
        SUM(case when INCOMETYPE =2 then od.PRICE else 0 end) amount_2
        from TICKET_ORDER o
        left join `TICKET_ORDERDETAIL` od on o.orderid=od.orderid
        left join TICKET_ORDER_INCOME_TYPE T_TYPE 
        ON o.PNRSOURCE = T_TYPE.PNRSOURCE 
        where od.CREATETIME>=%s
        and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) 
        and o.INTFLAG=0 AND IFNULL(od.`LINKTYPE`, 0) != 2 
        and od.REFUNDID=0
        and o.`MODE`=0  
        and OLD_ORDERID is null 
        and p like '%%hbgj%%'
        group by DEPCODE,ARRCODE;
    """

    gtgj_sql = """
        SELECT %s, 'gtgj',
        DEPCODE,ARRCODE,
        SUM(case when INCOMETYPE =0 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_0,
        SUM(case when INCOMETYPE =0 then 1 else 0 end) ticket_count_0,
        SUM(case when INCOMETYPE =0 then od.PRICE else 0 end) amount_0,
        SUM(case when INCOMETYPE =1 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_1,
        SUM(case when INCOMETYPE =1 then 1 else 0 end) ticket_count_1,
        SUM(case when INCOMETYPE =1 then od.PRICE else 0 end) amount_1,
        SUM(case when INCOMETYPE =2 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_2,
        SUM(case when INCOMETYPE =2 then 1 else 0 end) ticket_count_2,
        SUM(case when INCOMETYPE =2 then od.PRICE else 0 end) amount_2
        from TICKET_ORDER o
        left join `TICKET_ORDERDETAIL` od on o.orderid=od.orderid
        left join TICKET_ORDER_INCOME_TYPE T_TYPE 
        ON o.PNRSOURCE = T_TYPE.PNRSOURCE 
        where od.CREATETIME>=%s
        and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) 
        and o.INTFLAG=0 AND IFNULL(od.`LINKTYPE`, 0) != 2 
        and od.REFUNDID=0
        and o.`MODE`=0  
        and OLD_ORDERID is null 
        and p like '%%gtgj%%'
        group by DEPCODE,ARRCODE;
    """

    supply_sql = """
        
        SELECT %s, 'supply',
        DEPCODE,ARRCODE,
        SUM(case when INCOMETYPE =0 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_0,
        SUM(case when INCOMETYPE =0 then 1 else 0 end) ticket_count_0,
        SUM(case when INCOMETYPE =0 then od.PRICE else 0 end) amount_0,
        SUM(case when INCOMETYPE =1 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_1,
        SUM(case when INCOMETYPE =1 then 1 else 0 end) ticket_count_1,
        SUM(case when INCOMETYPE =1 then od.PRICE else 0 end) amount_1,
        SUM(case when INCOMETYPE =2 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_2,
        SUM(case when INCOMETYPE =2 then 1 else 0 end) ticket_count_2,
        SUM(case when INCOMETYPE =2 then od.PRICE else 0 end) amount_2
        from TICKET_ORDER o
        left join `TICKET_ORDERDETAIL` od on o.orderid=od.orderid
        left join TICKET_ORDER_INCOME_TYPE T_TYPE 
        ON o.PNRSOURCE = T_TYPE.PNRSOURCE 
        where od.CREATETIME>=%s
        and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) 
        and o.INTFLAG=0 AND IFNULL(od.`LINKTYPE`, 0) != 2 
        and od.REFUNDID=0
        and o.`MODE`=0  
        and OLD_ORDERID is null 
        and o.PNRSOURCE='supply' 
        group by DEPCODE,ARRCODE;
    """

    insert_sql = """
        insert into hbgj_client_airline_inland_weekly (s_day, client, depcode, arrcode, 
        profit_0, ticket_count_0, amount_0, profit_1, ticket_count_1, amount_1, profit_2,
        ticket_count_2, amount_2, createtime, updatetime
        ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        client = values(client),
        depcode = values(depcode),
        arrcode = values(arrcode),
        profit_0 = values(profit_0),
        ticket_count_0 = values(ticket_count_0),
        amount_0 = values(amount_0),
        profit_1 = values(profit_1),
        ticket_count_1 = values(ticket_count_1),
        amount_1 = values(amount_1),
        profit_2 = values(profit_2),
        ticket_count_2 = values(ticket_count_2),
        amount_2 = values(amount_2)
        
    """

    dto = [start_date, start_date, end_date]
    hbgj_data = DBCli().sourcedb_cli.query_all(hbgj_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, hbgj_data)

    gtgj_data = DBCli().sourcedb_cli.query_all(gtgj_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, gtgj_data)

    supply_data = DBCli().sourcedb_cli.query_all(supply_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, supply_data)


def update_hbgj_client_airline_inter_weekly():
    """hbgj gtgj supply各航线数据(国际), hbgj_client_airline_inter_weekly"""
    start_date, end_date = DateUtil.get_last_week_date()
    start_date = DateUtil.date2str(start_date, '%Y-%m-%d')
    end_date = DateUtil.date2str(end_date, '%Y-%m-%d')
    hbgj_sql = """
        SELECT %s, 'hbgj',
        DEPCODE,ARRCODE,
        SUM(case when INCOMETYPE =0 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_0,
        SUM(case when INCOMETYPE =0 then 1 else 0 end) ticket_count_0,
        SUM(case when INCOMETYPE =0 then od.PRICE else 0 end) amount_0,
        SUM(case when INCOMETYPE =1 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_1,
        SUM(case when INCOMETYPE =1 then 1 else 0 end) ticket_count_1,
        SUM(case when INCOMETYPE =1 then od.PRICE else 0 end) amount_1,
        SUM(case when INCOMETYPE =2 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_2,
        SUM(case when INCOMETYPE =2 then 1 else 0 end) ticket_count_2,
        SUM(case when INCOMETYPE =2 then od.PRICE else 0 end) amount_2
        from TICKET_ORDER o
        left join `TICKET_ORDERDETAIL` od on o.orderid=od.orderid
        left join TICKET_ORDER_INCOME_TYPE T_TYPE 
        ON o.PNRSOURCE = T_TYPE.PNRSOURCE 
        where od.CREATETIME>=%s
        and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) 
        and o.INTFLAG=1 AND IFNULL(od.`LINKTYPE`, 0) != 2 
        and od.REFUNDID=0
        and o.`MODE`=0  
        and OLD_ORDERID is null 
        and p like '%%hbgj%%'
        group by DEPCODE,ARRCODE;
    """

    gtgj_sql = """
        SELECT %s, 'gtgj',
        DEPCODE,ARRCODE,
        SUM(case when INCOMETYPE =0 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_0,
        SUM(case when INCOMETYPE =0 then 1 else 0 end) ticket_count_0,
        SUM(case when INCOMETYPE =0 then od.PRICE else 0 end) amount_0,
        SUM(case when INCOMETYPE =1 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_1,
        SUM(case when INCOMETYPE =1 then 1 else 0 end) ticket_count_1,
        SUM(case when INCOMETYPE =1 then od.PRICE else 0 end) amount_1,
        SUM(case when INCOMETYPE =2 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_2,
        SUM(case when INCOMETYPE =2 then 1 else 0 end) ticket_count_2,
        SUM(case when INCOMETYPE =2 then od.PRICE else 0 end) amount_2
        from TICKET_ORDER o
        left join `TICKET_ORDERDETAIL` od on o.orderid=od.orderid
        left join TICKET_ORDER_INCOME_TYPE T_TYPE 
        ON o.PNRSOURCE = T_TYPE.PNRSOURCE 
        where od.CREATETIME>=%s
        and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) 
        and o.INTFLAG=1 AND IFNULL(od.`LINKTYPE`, 0) != 2 
        and od.REFUNDID=0
        and o.`MODE`=0  
        and OLD_ORDERID is null 
        and p like '%%gtgj%%'
        group by DEPCODE,ARRCODE;
    """

    supply_sql = """

        SELECT %s, 'supply',
        DEPCODE,ARRCODE,
        SUM(case when INCOMETYPE =0 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_0,
        SUM(case when INCOMETYPE =0 then 1 else 0 end) ticket_count_0,
        SUM(case when INCOMETYPE =0 then od.PRICE else 0 end) amount_0,
        SUM(case when INCOMETYPE =1 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_1,
        SUM(case when INCOMETYPE =1 then 1 else 0 end) ticket_count_1,
        SUM(case when INCOMETYPE =1 then od.PRICE else 0 end) amount_1,
        SUM(case when INCOMETYPE =2 then od.PRICE + od.AIRPORTFEE + od.ratefee - od.OUTPAYPRICE else 0 end) profit_2,
        SUM(case when INCOMETYPE =2 then 1 else 0 end) ticket_count_2,
        SUM(case when INCOMETYPE =2 then od.PRICE else 0 end) amount_2
        from TICKET_ORDER o
        left join `TICKET_ORDERDETAIL` od on o.orderid=od.orderid
        left join TICKET_ORDER_INCOME_TYPE T_TYPE 
        ON o.PNRSOURCE = T_TYPE.PNRSOURCE 
        where od.CREATETIME>=%s
        and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) 
        and o.INTFLAG=1 AND IFNULL(od.`LINKTYPE`, 0) != 2 
        and od.REFUNDID=0
        and o.`MODE`=0  
        and OLD_ORDERID is null 
        and o.PNRSOURCE='supply' 
        group by DEPCODE,ARRCODE;
    """

    insert_sql = """
        insert into hbgj_client_airline_inter_weekly (s_day, client, depcode, arrcode, 
        profit_0, ticket_count_0, amount_0, profit_1, ticket_count_1, amount_1, profit_2,
        ticket_count_2, amount_2, createtime, updatetime
        ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        client = values(client),
        depcode = values(depcode),
        arrcode = values(arrcode),
        profit_0 = values(profit_0),
        ticket_count_0 = values(ticket_count_0),
        amount_0 = values(amount_0),
        profit_1 = values(profit_1),
        ticket_count_1 = values(ticket_count_1),
        amount_1 = values(amount_1),
        profit_2 = values(profit_2),
        ticket_count_2 = values(ticket_count_2),
        amount_2 = values(amount_2)

    """

    dto = [start_date, start_date, end_date]
    hbgj_data = DBCli().sourcedb_cli.query_all(hbgj_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, hbgj_data)

    gtgj_data = DBCli().sourcedb_cli.query_all(gtgj_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, gtgj_data)

    supply_data = DBCli().sourcedb_cli.query_all(supply_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, supply_data)


if __name__ == '__main__':
    # update_hbgj_client_airline_inland_weekly()
    import datetime
    last_week = datetime.date(2016, 12, 26)
    start_week, end_week = DateUtil.get_last_week_date()
    while start_week >= last_week:
        print start_week, end_week
        update_hbgj_client_airline_inter_weekly(start_week, end_week)
        start_week, end_week = DateUtil.get_last_week_date(start_week)
