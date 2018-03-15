# -*- coding: utf-8 -*-

from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_hb_insure_daily(days=0):
    """保险销售的投保率, operation_hbgj_insure"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1-days), '%Y-%m-%d')
    platform_sql = """
        SELECT left(od.CREATETIME,10),count(*),sum(od.OUTPAYPRICE)
        FROM `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER` o
        on od.ORDERID=o.ORDERID where od.CREATETIME BETWEEN
        %s and %s and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
        IFNULL(od.`LINKTYPE`, 0) != 2 GROUP BY left(od.CREATETIME,10)
    """

    update_platform_sql = """
            insert into operation_hbgj_insure (s_day, platform_ticket_num, platform_amount, createtime, updatetime)
            values (%s, %s ,%s, now(), now())
    """

    boat_sql = """
        SELECT DISTINCT count(*),left(INSURE_ORDERDETAIL.CREATETIME,10) FROM
        INSURE_ORDERDETAIL
         where createtime
        BETWEEN %s and %s and
        INSURE_ORDERDETAIL.insurecode in (select DISTINCT id from INSURE_DATA where bigtype=2)
        GROUP BY left(INSURE_ORDERDETAIL.CREATETIME,10);
    """

    update_boat_sql = """
        update operation_hbgj_insure set boat_order_num=%s where s_day = %s
    """

    refund_sql = """
        SELECT count(*), left(CREATETIME,10) FROM
        `INSURE_ORDERDETAIL`
        where createtime BETWEEN %s and %s and
        INSURE_ORDERDETAIL.insurecode in
        (select DISTINCT id from INSURE_DATA where bigtype=3)  GROUP BY left(CREATETIME,10);
    """

    update_refund_ticket_sql = """
        update operation_hbgj_insure set refund_ticket_order_num = %s where s_day = %s
    """

    delay_sql = """
        SELECT count(*), left(CREATETIME,10) FROM
        `INSURE_ORDERDETAIL`
         where createtime BETWEEN
        %s and %s and INSURE_ORDERDETAIL.insurecode in
        (select DISTINCT id from INSURE_DATA where bigtype=1) GROUP BY left(CREATETIME,10)
    """

    update_delay_sql = """
        update operation_hbgj_insure set delay_order_num=%s where s_day=%s
    """
    dto = [start_date, end_date]
    platform_data = DBCli().sourcedb_cli.query_one(platform_sql, dto)
    DBCli().targetdb_cli.insert(update_platform_sql, platform_data)

    boat_data = DBCli().sourcedb_cli.query_all(boat_sql, dto)
    DBCli().targetdb_cli.batch_insert(update_boat_sql, boat_data)

    refund_data = DBCli().sourcedb_cli.query_all(refund_sql, dto)
    DBCli().targetdb_cli.batch_insert(update_refund_ticket_sql, refund_data)

    delay_data = DBCli().sourcedb_cli.query_all(delay_sql, dto)
    DBCli().targetdb_cli.batch_insert(update_delay_sql, delay_data)
    pass


def update_insure_class_daily(days=0):
    """平台保险类型统计, operation_hbgj_insure_platform_daily"""
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1-days)
    hbgj_insure_sql = """
        SELECT
        left(i.createtime,10), "hbgj",
        i.insurecode, (SELECT DISTINCT bigtype from INSURE_DATA where id=i.insurecode) bigtype,
        count(case when o.intflag=1 then 1 end) inter_count,
        sum(case when o.intflag=1 then i.PRICE else 0 end) inter_price,
        count(case when o.intflag=0 then 1 end) inland_count,
        sum(case when o.intflag=0 then i.PRICE else 0 end) inland_price
        FROM skyhotel.`INSURE_ORDERDETAIL` i
        join skyhotel.`TICKET_ORDER` o on i.outorderid=o.orderid
        where i.createtime BETWEEN %s and %s
        and i.insurecode in (select DISTINCT id from INSURE_DATA where bigtype in (2, 1, 3, 5))  and o.p like '%%hbgj%%'
        GROUP BY left(i.createtime,10), i.insurecode order by bigtype;
    """

    gtgj_insure_sql = """
        SELECT
        left(i.createtime,10), "gtgj",
        i.insurecode, (SELECT DISTINCT bigtype from INSURE_DATA where id=i.insurecode) bigtype,
        count(case when o.intflag=1 then 1 end) inter_count,
        sum(case when o.intflag=1 then i.PRICE else 0 end) inter_price,
        count(case when o.intflag=0 then 1 end) inland_count,
        sum(case when o.intflag=0 then i.PRICE else 0 end) inland_price
        FROM skyhotel.`INSURE_ORDERDETAIL` i
        join skyhotel.`TICKET_ORDER` o on i.outorderid=o.orderid
        where i.createtime BETWEEN %s and %s
        and i.insurecode in (select DISTINCT id from INSURE_DATA where bigtype in (2, 1, 3, 5))  and o.p like '%%gtgj%%'
        GROUP BY left(i.createtime,10), i.insurecode order by bigtype;
    """

    other_insure_sql = """
        SELECT
        left(i.createtime,10), "else",
        i.insurecode, (SELECT DISTINCT bigtype from INSURE_DATA where id=i.insurecode) bigtype,
        count(case when o.intflag=1 then 1 end) inter_count,
        sum(case when o.intflag=1 then i.PRICE else 0 end) inter_price,
        count(case when o.intflag=0 then 1 end) inland_count,
        sum(case when o.intflag=0 then i.PRICE else 0 end) inland_price
        FROM skyhotel.`INSURE_ORDERDETAIL` i
        join skyhotel.`TICKET_ORDER` o on i.outorderid=o.orderid
        where i.createtime BETWEEN %s and %s
        and i.insurecode in (select DISTINCT id from INSURE_DATA where bigtype in (2, 1, 3, 5))
        and o.p not like '%%gtgj%%' and o.p not like '%%hbgj%%'
        GROUP BY left(i.createtime,10), i.insurecode order by bigtype;
    """

    insert_sql = """
        insert into operation_hbgj_insure_platform_daily (s_day, platform, insure_code, pid, inter_insure_num,
        inter_insure_amount, inland_insure_num, inland_insure_amount, createtime, updatetime)
        values (%s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        platform = values(platform),
        insure_code = values(insure_code),
        pid = values(pid),
        inter_insure_num = values(inter_insure_num),
        inter_insure_amount = values(inter_insure_amount),
        inland_insure_num = values(inland_insure_num),
        inland_insure_amount = values(inland_insure_amount)
    """

    dto = [start_date, end_date]
    hbgj_data = DBCli().sourcedb_cli.query_all(hbgj_insure_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, hbgj_data)

    gtgj_data = DBCli().sourcedb_cli.query_all(gtgj_insure_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, gtgj_data)

    other_data = DBCli().sourcedb_cli.query_all(other_insure_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, other_data)
    pass


def update_insure_type_daily(days=0):
    """航意险 退票险 延误险明细, operation_hbgj_insure_type_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 3))
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1-days))
    dto = [start_date, end_date]
    boat_sql = """
        SELECT left(i.createtime,10), i.insurecode,
        (SELECT DISTINCT bigtype from INSURE_DATA where id=i.insurecode) bigtype,
        count(*),sum(i.price)
        FROM `INSURE_ORDERDETAIL` i join `TICKET_ORDER` o
        on substring_index(i.OUTORDERID, 'P', -1)=o.orderid 
        where i.createtime >=%s and i.createtime <%s
        and i.insurecode in (select DISTINCT id from INSURE_DATA)
        GROUP BY left(i.createtime,10), i.insurecode;
    """

    insert_boat_sql = """
        insert into operation_hbgj_insure_type_daily (s_day, insure_code, pid, insure_order_num,
        insure_amount, insure_claim_num, insure_claim_amount, createtime, updatetime)
        values (%s, %s, %s, %s, %s, 0, 0, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        insure_code = values(insure_code),
        pid = values(pid),
        insure_order_num = values(insure_order_num),
        insure_amount = values(insure_amount),
        insure_claim_num = 0,
        insure_claim_amount = 0
    """
    boat_data = DBCli().sourcedb_cli.query_all(boat_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_boat_sql, boat_data)

    travel_sql = """
        SELECT left(i.createtime,10), i.insurecode,
        (SELECT DISTINCT bigtype from INSURE_DATA where id=i.insurecode) bigtype,
        count(*),sum(i.price)
        FROM `INSURE_ORDERDETAIL` i join `TICKET_ORDER` o
        on i.outorderid=o.orderid
        where i.createtime >=%s and i.createtime < %s
        and i.insurecode in (select DISTINCT id from INSURE_DATA where bigtype in (5))
        GROUP BY left(i.createtime,10), i.insurecode
    """

    travel_data = DBCli().sourcedb_cli.query_all(travel_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_boat_sql, travel_data)

    refund_order_amount_sql = """
            SELECT A.s_day,
                    ifnull(A.insurecode, 0),
                   (SELECT DISTINCT bigtype from INSURE_DATA where id=A.insurecode) bigtype,
                    ifnull(A.insure_order_num, 0),
                    ifnull(A.insure_amount, 0),
                    ifnull(A.claim_num, 0),
                    ifnull(A.claim_amount, 0) from (
            SELECT left(flydatetime,10) s_day, insurecode,
            count(case when `claim_price` is not null then 1 end) claim_num,
            sum(case when `claim_price` is not null then claim_price end) claim_amount,
            count(*) insure_order_num,
            sum(INSURE_ORDERDETAIL.price) insure_amount FROM
            `INSURE_ORDERDETAIL`
            where createtime >=%s and createtime<%s
            and INSURE_ORDERDETAIL.insurecode in
            (select DISTINCT id from INSURE_DATA where bigtype in (3))
            and flydatetime>=%s and flydatetime<%s group BY left(flydatetime,10), insurecode
            ) A
    """

    insert_refund_sql = """
        insert into operation_hbgj_insure_type_daily (s_day, insure_code, pid, insure_order_num,
        insure_amount, insure_claim_num, insure_claim_amount, createtime, updatetime)
        values (%s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        insure_code = values(insure_code),
        pid = values(pid),
        insure_order_num = values(insure_order_num),
        insure_amount = values(insure_amount),
        insure_claim_num = values(insure_claim_num),
        insure_claim_amount = values(insure_claim_amount)
    """
    import datetime
    refund_start_date = datetime.date(2016, 8, 15)
    refund_start_date = DateUtil.date2str(refund_start_date, '%Y-%m-%d')
    dto = [refund_start_date, end_date, start_date, end_date]
    refund_data = DBCli().sourcedb_cli.query_all(refund_order_amount_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_refund_sql, refund_data)

    delay_sql = """
        select A.s_day, A.insurecode,
        (SELECT DISTINCT bigtype from INSURE_DATA where id=A.insurecode) bigtype,
        ifnull(A.insure_num, 0),
        ifnull(A.insure_claim_num, 0) from (
        SELECT left(flydatetime ,10) s_day, insurecode,
        count(case when STATUS='7' then 1 end) insure_claim_num,
        count(*) insure_num
        FROM `INSURE_ORDERDETAIL`
        where flydatetime>=%s and flydatetime<%s
        and INSURE_ORDERDETAIL.insurecode
        in (select DISTINCT id from INSURE_DATA where bigtype=1)
        group BY left(flydatetime ,10), insurecode ) A
    """

    insert_delay_sql = """
        insert into operation_hbgj_insure_type_daily (s_day, insure_code, pid, insure_order_num,
        insure_amount, insure_claim_num, insure_claim_amount, createtime, updatetime)
        values (%s, %s, %s, %s, 0, %s, 0, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        insure_code = values(insure_code),
        pid = values(pid),
        insure_order_num = values(insure_order_num),
        insure_amount = 0,
        insure_claim_num = values(insure_claim_num),
        insure_claim_amount = 0
    """
    dto = [start_date, end_date]
    delay_data = DBCli().sourcedb_cli.query_all(delay_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_delay_sql, delay_data)
    pass


def update_hb_boat(days=0):
    """航意险投保详情和航意险退保详情, operation_accident_insure_detail_daily operation_accident_insure_refund_detail_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days*1), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1-days), '%Y-%m-%d')
    dto = [start_date, end_date]
    detail_sql = """
        SELECT DATE_FORMAT(i.createtime, '%%Y-%%m-%%d') s_day,
        sum(case when o.P like '%%hbgj%%' and o.INTFLAG=0 then 1 else 0 END) insuer_hbgj_inland_count,
        sum(case when o.P like '%%hbgj%%' and o.INTFLAG=1 then 1 else 0 END) insuer_hbgj_inter_count,
        sum(case when o.P like '%%gtgj%%' and o.INTFLAG=0 then 1 else 0 END) insuer_gtgj_inland_count,
        sum(case when o.P like '%%gtgj%%' and o.INTFLAG=1 then 1 else 0 END) insuer_gtgj_inter_count,
        sum(case when o.INTFLAG=0 then 1 else 0 END) insuer_inland_count,
        sum(case when o.INTFLAG=1 then 1 else 0 END) insuer_inter_count,
        sum(case when o.P like '%%hbgj%%' and o.INTFLAG=0 then i.PRICE else 0 END) insuer_hbgj_inland_amount,
        sum(case when o.P like '%%hbgj%%' and o.INTFLAG=1 then i.PRICE else 0 END) insuer_hbgj_inter_amount,
        sum(case when o.P like '%%gtgj%%' and o.INTFLAG=0 then i.PRICE else 0 END) insuer_gtgj_inland_amount,
        sum(case when o.P like '%%gtgj%%' and o.INTFLAG=1 then i.PRICE else 0 END) insuer_gtgj_inter_amount,
        sum(case when o.INTFLAG=0 then i.PRICE else 0 END) insuer_inland_amount,
        sum(case when o.INTFLAG=1 then i.PRICE else 0 END) insuer_inter_amount
        FROM `INSURE_ORDERDETAIL` i
        join `TICKET_ORDER` o on substring_index(i.OUTORDERID, 'P', -1)=o.ORDERID
        where i.CREATETIME>=%s
        and i.CREATETIME<%s
        and i.insurecode in (select distinct id from INSURE_DATA where bigtype in (2) and id !='PA_A020')
        GROUP BY s_day;
    """

    insert_sql = """
        insert into operation_accident_insure_detail_daily (s_day, insuer_hbgj_inland_count, insuer_hbgj_inter_count, insuer_gtgj_inland_count
                ,insuer_gtgj_inter_count, insuer_inland_count, insuer_inter_count, insuer_hbgj_inland_amount, insuer_hbgj_inter_amount
                ,insuer_gtgj_inland_amount, insuer_gtgj_inter_amount, insuer_inland_amount, insuer_inter_amount,
                createtime, updatetime
                ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                on duplicate key update updatetime = now(),
                s_day = values(s_day),
                insuer_hbgj_inland_count = values(insuer_hbgj_inland_count),
                insuer_hbgj_inter_count = values(insuer_hbgj_inter_count),
                insuer_gtgj_inland_count = values(insuer_gtgj_inland_count),
                insuer_gtgj_inter_count = values(insuer_gtgj_inter_count),
                insuer_inland_count = values(insuer_inland_count),
                insuer_inter_count = values(insuer_inter_count),
                insuer_hbgj_inland_amount = values(insuer_hbgj_inland_amount),
                insuer_hbgj_inter_amount = values(insuer_hbgj_inter_amount),
                insuer_gtgj_inland_amount = values(insuer_gtgj_inland_amount),
                insuer_gtgj_inter_amount = values(insuer_gtgj_inter_amount),
                insuer_inland_amount = values(insuer_inland_amount),
                insuer_inter_amount = values(insuer_inter_amount)

    """
    detail_data = DBCli().sourcedb_cli.query_all(detail_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, detail_data)

    detail_refund_sql = """
        SELECT DATE_FORMAT(i.updatetime, '%%Y-%%m-%%d') s_day,
        sum(case when o.P LIKE '%%hbgj%%' and o.INTFLAG=0 then 1 else 0 end) insuer_refund_hbgj_inland,
        sum(case when o.P LIKE '%%hbgj%%' and o.INTFLAG=1 then 1 else 0 end) insuer_refund_hbgj_inter,
        sum(case when o.P LIKE '%%gtgj%%' and o.INTFLAG=0 then 1 else 0 end) insuer_refund_gtgj_inland,
        sum(case when o.P LIKE '%%gtgj%%' and o.INTFLAG=1 then 1 else 0 end) insuer_refund_gtgj_inter,
        sum(case when o.INTFLAG=0 then 1 else 0 end) insuer_refund_inland,
        sum(case when o.INTFLAG=1 then 1 else 0 end) insuer_refund_inter,
        sum(case when o.P LIKE '%%hbgj%%' and o.INTFLAG=0 then i.PRICE else 0 end) insuer_refund_hbgj_inland_amount,
        sum(case when o.P LIKE '%%hbgj%%' and o.INTFLAG=1 then i.PRICE else 0 end) insuer_refund_hbgj_inter_amount,
        sum(case when o.P LIKE '%%gtgj%%' and o.INTFLAG=0 then i.PRICE else 0 end) insuer_refund_gtgj_inland_amount,
        sum(case when o.P LIKE '%%gtgj%%' and o.INTFLAG=1 then i.PRICE else 0 end) insuer_refund_gtgj_inter_amount,
        sum(case when o.INTFLAG=0 then i.PRICE else 0 end) insuer_refund_inland_amount,
        sum(case when o.INTFLAG=1 then i.PRICE else 0 end) insuer_refund_inter_amount
        FROM `INSURE_ORDERDETAIL` i
        join `TICKET_ORDER` o on substring_index(i.OUTORDERID, 'P', -1)=o.ORDERID
        where i.updatetime>=%s
        and i.updatetime<%s
        and i.insurecode in (select distinct id from INSURE_DATA where bigtype in (2) and id !='PA_A020')
        and i.STATUS in ('21','2')
        GROUP BY s_day;
    """

    insert_sql = """
            insert into operation_accident_insure_refund_detail_daily (s_day, insuer_refund_hbgj_inland, insuer_refund_hbgj_inter,
                insuer_refund_gtgj_inland, insuer_refund_gtgj_inter, insuer_refund_inland, insuer_refund_inter, insuer_refund_hbgj_inland_amount,
                insuer_refund_hbgj_inter_amount, insuer_refund_gtgj_inland_amount, insuer_refund_gtgj_inter_amount,
                insuer_refund_inland_amount, insuer_refund_inter_amount, createtime, updatetime
            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
            on duplicate key update updatetime = now(),
            s_day = values(s_day),
            insuer_refund_hbgj_inland = values(insuer_refund_hbgj_inland),
            insuer_refund_hbgj_inter = values(insuer_refund_hbgj_inter),
            insuer_refund_gtgj_inland = values(insuer_refund_gtgj_inland),
            insuer_refund_gtgj_inter = values(insuer_refund_gtgj_inter),

            insuer_refund_inland = values(insuer_refund_inland),
            insuer_refund_inter = values(insuer_refund_inter),
            insuer_refund_hbgj_inland_amount = values(insuer_refund_hbgj_inland_amount),
            insuer_refund_hbgj_inter_amount = values(insuer_refund_hbgj_inter_amount),
            insuer_refund_gtgj_inland_amount = values(insuer_refund_gtgj_inland_amount),
            insuer_refund_gtgj_inter_amount = values(insuer_refund_gtgj_inter_amount),
            insuer_refund_inland_amount = values(insuer_refund_inland_amount),
            insuer_refund_inter_amount = values(insuer_refund_inter_amount)
    """
    detail_refund_data = DBCli().sourcedb_cli.query_all(detail_refund_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_sql, detail_refund_data)


def update_insure_type_detail_daily(days=0):
    """航意险 退票险 延误险国际机票的保险各种指标, operation_hbgj_insure_type_detail_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 1), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - days), '%Y-%m-%d')
    dto = [start_date, end_date]
    boat_sql = """
        SELECT left(i.createtime,10), i.insurecode,
        (SELECT DISTINCT bigtype from INSURE_DATA where id=i.insurecode) bigtype,
        count(DISTINCT o.phoneid),
        count(*),sum(i.price),
        sum(case
            when i.status in (2, 21) then 1 else 0
        end) refund_num,
        sum(case
            when i.status in (2, 21) then i.price else 0
        end) refund_num
        FROM `INSURE_ORDERDETAIL` i join `TICKET_ORDER` o
        on substring_index(i.OUTORDERID, 'P', -1)=o.orderid
        where i.createtime >=%s and i.createtime <%s
        and o.INTFLAG=1
        and i.insurecode in (select DISTINCT id from INSURE_DATA)
        GROUP BY left(i.createtime,10), i.insurecode;
    """

    insert_boat_sql = """
        insert into operation_hbgj_insure_type_detail_daily (s_day, insure_code, pid, insure_holder_num, insure_order_num,
        insure_amount, insure_refund_num, insure_refund_amount,
        insure_claim_num, insure_claim_amount, createtime, updatetime)
        values (%s, %s, %s, %s, %s, %s,%s, %s, 0, 0, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        insure_code = values(insure_code),
        pid = values(pid),
        insure_holder_num = values(insure_holder_num),
        insure_order_num = values(insure_order_num),
        insure_amount = values(insure_amount),
        insure_refund_num = values(insure_refund_num),
        insure_refund_amount = values(insure_refund_amount),
        insure_claim_num = 0,
        insure_claim_amount = 0
    """
    boat_data = DBCli().sourcedb_cli.query_all(boat_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_boat_sql, boat_data)

    travel_sql = """
        SELECT left(i.createtime,10), i.insurecode,
        (SELECT DISTINCT bigtype from INSURE_DATA where id=i.insurecode) bigtype,
        count(DISTINCT o.phoneid),
        count(*),sum(i.price),
        sum(case
            when i.status in (2, 21) then 1 else 0
        end) refund_num,
        sum(case
            when i.status in (2, 21) then i.price else 0
        end) refund_amount
        FROM `INSURE_ORDERDETAIL` i join `TICKET_ORDER` o
        on i.outorderid=o.orderid
        where i.createtime >=%s and i.createtime <%s
        and o.INTFLAG=1
        and i.insurecode in (select DISTINCT id from INSURE_DATA where bigtype in (5))
        GROUP BY left(i.createtime,10), i.insurecode
    """

    travel_data = DBCli().sourcedb_cli.query_all(travel_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_boat_sql, travel_data)

    refund_order_amount_sql = """

            SELECT A.s_day,
                ifnull(A.insurecode, 0),
               (SELECT DISTINCT bigtype from INSURE_DATA where id=A.insurecode) bigtype,
                A.phone_num,
                                    ifnull(A.insure_order_num, 0),
                ifnull(A.insure_amount, 0),
                                    A.refund_num, A.refund_amount,
                ifnull(A.claim_num, 0),
                    ifnull(A.claim_amount, 0) from (
            SELECT left(flydatetime,10) s_day, insurecode,
            count(case when `claim_price` is not null then 1 end) claim_num,
            sum(case when `claim_price` is not null then claim_price end) claim_amount,
            count(*) insure_order_num,
                    sum(case
                        when i.status in (2, 21) then 1 else 0
                    end) refund_num,
                    sum(case
                        when i.status in (2, 21) then i.price else 0
                    end) refund_amount,
                    count(DISTINCT o.phoneid) phone_num,
            sum(i.price) insure_amount FROM
            `INSURE_ORDERDETAIL` i
        join `TICKET_ORDER` o
        on substring_index(i.OUTORDERID, 'P', -1)=o.orderid
            where i.createtime >=%s and i.createtime<%s
            and i.insurecode in
            (select DISTINCT id from INSURE_DATA where bigtype in (3))
            and o.INTFLAG=1
            and flydatetime>=%s and flydatetime<%s group BY left(flydatetime,10), insurecode
            ) A;
    """

    insert_refund_sql = """
        insert into operation_hbgj_insure_type_detail_daily (s_day, insure_code, pid, insure_holder_num, insure_order_num,
        insure_amount,
        insure_refund_num, insure_refund_amount,
        insure_claim_num, insure_claim_amount, createtime, updatetime)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        insure_code = values(insure_code),
        pid = values(pid),
        insure_holder_num = values(insure_holder_num),
        insure_order_num = values(insure_order_num),
        insure_amount = values(insure_amount),
        insure_refund_num = values(insure_refund_num),
        insure_refund_amount = values(insure_refund_amount),
        insure_claim_num = values(insure_claim_num),
        insure_claim_amount = values(insure_claim_amount)
    """
    import datetime
    refund_start_date = datetime.date(2016, 8, 15)
    refund_start_date = DateUtil.date2str(refund_start_date, '%Y-%m-%d')
    dto = [refund_start_date, end_date, start_date, end_date]
    refund_data = DBCli().sourcedb_cli.query_all(refund_order_amount_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_refund_sql, refund_data)

    delay_sql = """
        select A.s_day, A.insurecode,
        (SELECT DISTINCT bigtype from INSURE_DATA where id=A.insurecode) bigtype,
        A.phone_num,
        ifnull(A.insure_num, 0), A.refund_num, A.refund_amount,
        ifnull(A.insure_claim_num, 0) from (
        SELECT left(flydatetime ,10) s_day, insurecode,
        count(case when STATUS='7' then 1 end) insure_claim_num,
        count(*) insure_num,
        count(DISTINCT o.phoneid) phone_num,
        sum(case
            when i.status in (2, 21) then 1 else 0
        end) refund_num,
        sum(case
            when i.status in (2, 21) then i.price else 0
        end) refund_amount
        FROM `INSURE_ORDERDETAIL` i
        join `TICKET_ORDER` o
        on substring_index(i.OUTORDERID, 'P', -1)=o.orderid
        where i.flydatetime>=%s and i.flydatetime<%s
        and i.insurecode
        in (select DISTINCT id from INSURE_DATA where bigtype=1)
        and o.INTFLAG=1
        group BY left(i.flydatetime ,10), i.insurecode ) A
    """

    insert_delay_sql = """
        insert into operation_hbgj_insure_type_detail_daily (s_day, insure_code, pid, insure_holder_num, insure_order_num,
        insure_amount, insure_refund_num, insure_refund_amount, insure_claim_num, insure_claim_amount, createtime, updatetime)
        values (%s, %s, %s, %s, %s, 0, %s, %s, %s, 0, now(), now())
        on duplicate key update updatetime = now(),
        s_day = values(s_day),
        insure_code = values(insure_code),
        pid = values(pid),
        insure_holder_num  = values(insure_holder_num),
        insure_order_num = values(insure_order_num),
        insure_amount = 0,
        insure_refund_num = values(insure_refund_num),
        insure_refund_amount = values(insure_refund_amount),
        insure_claim_num = values(insure_claim_num),
        insure_claim_amount = 0
    """
    dto = [start_date, end_date]
    delay_data = DBCli().sourcedb_cli.query_all(delay_sql, dto)
    DBCli().targetdb_cli.batch_insert(insert_delay_sql, delay_data)

    rate_sql = """
        select INSUREID, rate from TICKET_INSURE_INCOME_RULE
    """
    profile_sql = """
        select insure_code,insure_amount, insure_refund_amount from operation_hbgj_insure_type_detail_daily 
        where s_day=%s
    """
    rate_data = DBCli().sourcedb_cli.query_all(rate_sql)
    insure_profit = DBCli().targetdb_cli.query_all(profile_sql, [start_date, ])
    rate_dict = dict(rate_data)
    update_insure_profit_sql = """
        update operation_hbgj_insure_type_detail_daily set insure_profit=%s, insurance_amount=%s 
        where s_day=%s and insure_code=%s
    """
    insure_profit_data = []
    for ip in insure_profit:
        insure_code, insure_amount, refund_amount = ip
        insure_rate = rate_dict.get(insure_code, None)
        insurance_amount = insure_amount - refund_amount
        if insure_rate:
            insure_profit = float(str(insurance_amount)) * float(insure_rate)
        else:
            insure_profit = insure_amount
        insure_profit_data.append([insure_profit, insurance_amount, start_date, insure_code])
    DBCli().targetdb_cli.batch_insert(update_insure_profit_sql, insure_profit_data)


if __name__ == "__main__":
    i = 1
    while i <= 15:
        update_insure_type_detail_daily(i)
        i += 1
