# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from collections import defaultdict


def update_hbgj_channel_client_ticket_daily(days=1):
    """航班客户端渠道机票统计, hbgj_channel_client_ticket_daily"""
    start_date = DateUtil.get_date_before_days(days * 1)
    end_date = DateUtil.get_date_after_days(1 - days)
    start_date = str(start_date)
    register_users_sql = """
        SELECT DATE_FORMAT(createtime, '%%Y-%%m-%%d') s_day,
        sum(case when p like '%%hbgj%%' then 1 else 0 end) 航班管家注册用户,
        sum(case when p like '%%gtgj%%' and p not like '%%wxapplet%%' then 1 else 0 end) 高铁管家注册用户,
        sum(case when p like '%%wxapplet%%' then 1 else 0 end) 高铁管家小程序注册用户,
        sum(case when p not like '%%gtgj%%' and p not like '%%hbgj%%' then 1 else 0 end) 微信等其他注册用户
        FROM `phone_user` WHERE `CREATETIME`>= %s
        and `CREATETIME`< %s
    """

    order_sql = """
        SELECT DATE_FORMAT(o.createtime, '%%Y-%%m-%%d') s_day,SALETYPE,o.PNRSOURCE, 
        sum(case when p like '%%hbgj%%' then 1 else 0 end) 航班管家机票订单数, 
        sum(case when p like '%%gtgj%%' and p not like '%%wxapplet%%' then 1 else 0 end) 高铁管家机票订单数,
        sum(case when p  like '%%wxapplet%%' then 1 else 0 end) 高铁小程序机票订单数,
        sum(case when p not like '%%gtgj%%' and p not like '%%hbgj%%' then 1 else 0 end) 微信等其他机票订单数
        FROM TICKET_ORDER o join PNRSOURCE_CONFIG c on o.PNRSOURCE=c.PNRSOURCE 
        WHERE o.CREATETIME>=%s and o.CREATETIME<%s and INTFLAG=0 
        GROUP BY s_day,SALETYPE, o.PNRSOURCE ;
    """

    success_order_ticket_sql = """
        SELECT DATE_FORMAT(od.createtime, '%%Y-%%m-%%d') s_day,SALETYPE, 
        o.PNRSOURCE,count(DISTINCT(case when p like '%%hbgj%%' then o.ORDERID  end)) 航班管家机票成功订单数,
        count(DISTINCT(case when p like '%%gtgj%%' and p not like '%%wxapplet%%' then o.ORDERID  end)) 高铁管家机票成功订单数,
        count(DISTINCT(case when p like '%%wxapplet%%' then o.ORDERID  end)) 高铁小程序机票成功订单数,
        count(DISTINCT(case when p not like '%%gtgj%%' and p not like '%%hbgj%%' then o.ORDERID  end)) 微信等其他机票成功订单数
        FROM `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        join PNRSOURCE_CONFIG c on o.PNRSOURCE=c.PNRSOURCE
        where od.CREATETIME>=%s and od.CREATETIME<%s 
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
        IFNULL(od.`LINKTYPE`, 0) != 2 and INTFLAG=0 GROUP BY s_day,SALETYPE, o.PNRSOURCE;
    """

    ticket_sql = """
        SELECT DATE_FORMAT(od.createtime, '%%Y-%%m-%%d') s_day,SALETYPE,o.PNRSOURCE, 
        sum(case when p like '%%hbgj%%' then 1 else 0 end) 航班管家机票成功出票数, 
        sum(case when p like '%%gtgj%%' and p not like '%%wxapplet%%' then 1 else 0 end) 高铁管家机票成功出票数,
        sum(case when p  like '%%wxapplet%%' then 1 else 0 end) 高铁小程序机票成功出票数,
        sum(case when p not like '%%gtgj%%' and p not like '%%hbgj%%' then 1 else 0 end) 微信等其他机票成功出票数
        FROM `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        join PNRSOURCE_CONFIG c on o.PNRSOURCE=c.PNRSOURCE
        where od.CREATETIME>=%s and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
        IFNULL(od.`LINKTYPE`, 0) != 2 and INTFLAG=0 GROUP BY s_day,SALETYPE,o.PNRSOURCE;

    """

    boat_insure_sql = """
        SELECT DATE_FORMAT(od.createtime, '%%Y-%%m-%%d') s_day,SALETYPE,o.PNRSOURCE,
        count(DISTINCT(case when p like '%%hbgj%%' then i.insureid  end)) 航班管家机票成功保单数, 
        count(DISTINCT(case when p like '%%gtgj%%' and p not like '%%wxapplet%%' then i.insureid  end)) 高铁管家机票成功保单数,
        count(DISTINCT(case when p  like '%%wxapplet%%' then i.insureid  end)) 高铁小程序机票成功保单数,
        count(DISTINCT(case when p not like '%%gtgj%%' and p not like '%%hbgj%%' then i.insureid  end)) 微信等其他机票成功保单数
        FROM `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        join PNRSOURCE_CONFIG c on o.PNRSOURCE=c.PNRSOURCE
        join INSURE_ORDERDETAIL i on o.ORDERID=i.outorderid
        where od.CREATETIME>=%s and od.CREATETIME<%s 
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
        IFNULL(od.`LINKTYPE`, 0) != 2 and INTFLAG=0 GROUP BY s_day,SALETYPE,o.PNRSOURCE
        and i.insurecode in (
        select DISTINCT id from INSURE_DATA where bigtype in (2));

    """

    web_insure_sql = """
        SELECT DATE_FORMAT(od.createtime, '%%Y-%%m-%%d') s_day,SALETYPE,o.PNRSOURCE,
        count(DISTINCT(case when p like '%%hbgj%%' then i.insureid  end)) 航班管家机票成功保单数, 
        count(DISTINCT(case when p like '%%gtgj%%' and p not like '%%wxapplet%%' then i.insureid  end)) 高铁管家机票成功保单数,
        count(DISTINCT(case when p  like '%%wxapplet%%' then i.insureid  end)) 高铁小程序机票成功保单数,
        count(DISTINCT(case when p not like '%%gtgj%%' and p not like '%%hbgj%%' then i.insureid  end)) 微信等其他机票成功保单数
        FROM `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        join PNRSOURCE_CONFIG c on o.PNRSOURCE=c.PNRSOURCE
        join INSURE_ORDERDETAIL i on CONCAT('P',o.ORDERID)=i.OUTORDERID
        where od.CREATETIME>=%s and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
        IFNULL(od.`LINKTYPE`, 0) != 2 and INTFLAG=0
        and i.insurecode in (
        select DISTINCT id from INSURE_DATA where bigtype in (2)) GROUP BY s_day,SALETYPE,o.PNRSOURCE;

    """

    delay_insure_sql = """
        SELECT flydate,SALETYPE, o.PNRSOURCE,
        count(DISTINCT(case when p like '%%hbgj%%' then o.ORDERID  end)) 航班管家机票成功出票数, 
        count(DISTINCT(case when p like '%%gtgj%%' and p not like '%%wxapplet%%' then o.ORDERID  end)) 高铁管家机票成功出票数,
        count(DISTINCT(case when p  like '%%wxapplet%%' then o.ORDERID  end)) 高铁小程序机票成功出票数,
        count(DISTINCT(case when p not like '%%gtgj%%' and p not like '%%hbgj%%' then o.ORDERID  end)) 微信等其他机票成功出票数
         FROM `TICKET_DELAY_CARE` d 
        join TICKET_ORDER o on d.ORDERID=o.ORDERID
        join PNRSOURCE_CONFIG c on o.PNRSOURCE=c.PNRSOURCE
        WHERE flydate>=%s and flydate<%s and state='1' and INTFLAG=0 GROUP BY SALETYPE;
    """

    delay_accquire_sql = """
    
        SELECT flydate,SALETYPE,c.PNRSOURCE,
        count(DISTINCT(case when p like '%%hbgj%%' then o.ORDERID  end)) 航班管家机票成功出票数, 
        count(DISTINCT(case when p like '%%gtgj%%' and p not like '%%wxapplet%%' then o.ORDERID  end)) 高铁管家机票成功出票数,
        count(DISTINCT(case when p  like '%%wxapplet%%' then o.ORDERID  end)) 高铁小程序机票成功出票数,
        count(DISTINCT(case when p not like '%%gtgj%%' and p not like '%%hbgj%%' then o.ORDERID  end)) 微信等其他机票成功出票数
         FROM `TICKET_DELAY_CARE` d 
        join TICKET_ORDER o on d.ORDERID=o.ORDERID
        join PNRSOURCE_CONFIG c on o.PNRSOURCE=c.PNRSOURCE
        WHERE flydate>=%s and flydate<%s and state='1' and INTFLAG=0 and chargetime<>0 and chargenum!=0 GROUP BY SALETYPE;

    """

    consumers_sql = """
        SELECT DATE_FORMAT(od.createtime, '%%Y-%%m-%%d') s_day,SALETYPE,o.PNRSOURCE,
        count(DISTINCT(case when p like '%%hbgj%%' then PHONEID  end)) 航班管家机票新增消费用户数,
        count(DISTINCT(case when p like '%%gtgj%%' and p not like '%%wxapplet%%' then PHONEID  end)) 高铁管家机票新增消费用户数,
        count(DISTINCT(case when p like '%%wxapplet%%' then PHONEID  end)) 高铁小程序机票新增消费用户数,
        count(DISTINCT(case when p not like '%%gtgj%%' and p not like '%%hbgj%%' then PHONEID end)) 微信等其他机票新增消费用户数
        FROM `TICKET_ORDERDETAIL` od 
        INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        join PNRSOURCE_CONFIG c on o.PNRSOURCE=c.PNRSOURCE
        where od.CREATETIME>=%s and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) 
        AND IFNULL(od.`LINKTYPE`, 0) != 2 
        and INTFLAG=0 and FIRSTPAY=1  
        GROUP BY s_day,SALETYPE,PNRSOURCE;

    """
    insert_data = defaultdict(list)
    total_insert_data = defaultdict(list)
    reg_users_dict = {}
    dto = [start_date, end_date]
    register_users = DBCli().sourcedb_cli.query_all(register_users_sql, dto)
    for reg in register_users:
        s_day, hbgj_users, gt_users, gt_weixin, weixin = reg
        reg_users_dict[u"hb-注册用户"] = hbgj_users
        reg_users_dict[u"gt-注册用户"] = gt_users
        reg_users_dict[u"wx-注册用户"] = weixin
        reg_users_dict[u"gtwx-注册用户"] = gt_weixin

    order = DBCli().sourcedb_cli.query_all(order_sql, dto)

    insert_keys = ["hb-zx", "gt-zx", "wx-zx", "gtwx-zx",
                   "hb-zy", "gt-zy", "wx-zy", "gtwx-zy",
                   "hb-hzf", "gt-hzf", "wx-hzf", "gtwx-hzf",
                   "hb-gwdg", "gt-gwdg", "wx-gwdg", "gtwx-gwdg"
                   ]

    def map_fun(sale_type, pn, arg):
        hb = arg[3]
        gt = arg[4]
        gt_wx = arg[5]
        wx = arg[6]
        if str(sale_type) == "10":
            insert_data["hb-zx"].append(hb)
            insert_data["gt-zx"].append(gt)
            insert_data["gtwx-zx"].append(gt_wx)
            insert_data["wx-zx"].append(wx)
        elif str(sale_type) == "13" or pn == "hlth":
            insert_data["hb-zy"].append(hb)
            insert_data["gt-zy"].append(gt)
            insert_data["gtwx-zy"].append(gt_wx)
            insert_data["wx-zy"].append(wx)
        elif str(sale_type) == "12" and pn != "hlth":
            insert_data["hb-hzf"].append(hb)
            insert_data["gt-hzf"].append(gt)
            insert_data["gtwx-hzf"].append(gt_wx)
            insert_data["wx-hzf"].append(wx)
        elif str(sale_type) == "11" or str(sale_type) == "15":
            insert_data["hb-gwdg"].append(hb)
            insert_data["gt-gwdg"].append(gt)
            insert_data["gtwx-gwdg"].append(gt_wx)
            insert_data["wx-gwdg"].append(wx)
        else:
            print "error"
            print arg

    def data_do_clear():
        for k, v in insert_data.items():
            total_insert_data[k].append(sum(v))

        for tt in set(insert_keys).difference(set(insert_data.keys())):
            total_insert_data[tt].append(0)
        insert_data.clear()

    # 机票订单数
    for o in order:
        map_fun(o[1], o[2], o)

    data_do_clear()

    # 机票成功订单数
    success_order_ticket = DBCli().sourcedb_cli.query_all(success_order_ticket_sql, dto)
    for s in success_order_ticket:
        map_fun(s[1], s[2], s)

    data_do_clear()
    # #
    # 出票数
    ticket_data = DBCli().sourcedb_cli.query_all(ticket_sql, dto)
    for td in ticket_data:
        map_fun(td[1], td[2], td)

    data_do_clear()
    # 航意险保单量
    boat_insure_data = DBCli().sourcedb_cli.query_all(boat_insure_sql, dto)
    for bid in boat_insure_data:
        map_fun(bid[1], bid[2], bid)

    data_do_clear()
    # 官网+英行航意险套餐保单量
    web_insure_data = DBCli().sourcedb_cli.query_all(web_insure_sql, dto)
    for wid in web_insure_data:
        map_fun(wid[1], wid[2], wid)

    data_do_clear()
    # 延误宝理赔
    delay_insure_data = DBCli().sourcedb_cli.query_all(delay_insure_sql, dto)
    for did in delay_insure_data:
        map_fun(did[1], did[2], did)

    data_do_clear()

    # 延误宝获得
    delay_accquire_data = DBCli().sourcedb_cli.query_all(delay_accquire_sql, dto)
    for dad in delay_accquire_data:
        map_fun(dad[1], dad[2], dad)

    data_do_clear()
    # 消费新用户
    consumers_data = DBCli().sourcedb_cli.query_all(consumers_sql, dto)
    for cd in consumers_data:
        map_fun(cd[1], cd[2], cd)

    data_do_clear()

    last_insert_data = list()

    for k, v in total_insert_data.items():
        k = k.split("-")
        if k[0] == "hb":
            if k[1] == "zx":
                v.insert(0, 11)
                v.insert(0, "直销")
                v.insert(0, "航班管家")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "zy":
                v.insert(0, 12)
                v.insert(0, "自营")
                v.insert(0, "航班管家")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "hzf":
                v.insert(0, 13)
                v.insert(0, "合作方")
                v.insert(0, "航班管家")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "gwdg":
                v.insert(0, 14)
                v.insert(0, "官网代购")
                v.insert(0, "航班管家")
                v.insert(0, start_date)
                last_insert_data.append(v)
        elif k[0] == "gt":
            if k[1] == "zx":
                v.insert(0, 21)
                v.insert(0, "直销")
                v.insert(0, "高铁管家")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "zy":
                v.insert(0, 22)
                v.insert(0, "自营")
                v.insert(0, "高铁管家")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "hzf":
                v.insert(0, 23)
                v.insert(0, "合作方")
                v.insert(0, "高铁管家")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "gwdg":
                v.insert(0, 24)
                v.insert(0, "官网代购")
                v.insert(0, "高铁管家")
                v.insert(0, start_date)
                last_insert_data.append(v)
        elif k[0] == "wx":
            if k[1] == "zx":
                v.insert(0, 31)
                v.insert(0, "直销")
                v.insert(0, "微信")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "zy":
                v.insert(0, 32)
                v.insert(0, "自营")
                v.insert(0, "微信")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "hzf":
                v.insert(0, 33)
                v.insert(0, "合作方")
                v.insert(0, "微信")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "gwdg":
                v.insert(0, 34)
                v.insert(0, "官网代购")
                v.insert(0, "微信")
                v.insert(0, start_date)
                last_insert_data.append(v)
        elif k[0] == "gtwx":
            if k[1] == "zx":
                v.insert(0, 41)
                v.insert(0, "直销")
                v.insert(0, "高铁微信小程序")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "zy":
                v.insert(0, 42)
                v.insert(0, "自营")
                v.insert(0, "高铁微信小程序")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "hzf":
                v.insert(0, 43)
                v.insert(0, "合作方")
                v.insert(0, "高铁微信小程序")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "gwdg":
                v.insert(0, 44)
                v.insert(0, "官网代购")
                v.insert(0, "高铁微信小程序")
                v.insert(0, start_date)
                last_insert_data.append(v)

    insert_sql = """
        insert into operation_client_channel_ticket_daily
        (s_day, client, channel, pid, register_users, order_nums, order_success_nums, ticket_num
        ,boat_nums, website_boat_nums, delay_insure_nums, delay_obtain_nums
        ,consumers,create_time, update_time)
        values
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
    """

    new_last_insert_data = list()
    for i in last_insert_data:
        tmp = i
        tmp.insert(4, 0)
        new_last_insert_data.append(tmp)

    new_last_insert_data.append([start_date, "航班管家", "合计", 10, reg_users_dict[u"hb-注册用户"], 0, 0, 0, 0, 0, 0, 0, 0])
    new_last_insert_data.append([start_date, "高铁管家", "合计", 20, reg_users_dict[u"gt-注册用户"], 0, 0, 0, 0, 0, 0, 0, 0])
    new_last_insert_data.append([start_date, "微信", "合计", 30, reg_users_dict[u"wx-注册用户"], 0, 0, 0, 0, 0, 0, 0, 0])
    new_last_insert_data.append([start_date, "高铁微信小程序", "合计", 40, reg_users_dict[u"gtwx-注册用户"], 0, 0, 0, 0, 0, 0, 0, 0])

    DBCli().targetdb_cli.batch_insert(insert_sql, new_last_insert_data)
    update_client_channel_hj(start_date)


def update_client_channel_hj(s_day):
    sql = """
        select sum(order_nums), sum(order_success_nums),
        sum(ticket_num), sum(boat_nums), sum(website_boat_nums),
        sum(delay_insure_nums), sum(delay_obtain_nums), sum(consumers),
        s_day, client from operation_client_channel_ticket_daily 
        where s_day=%s and channel !="合计"
        group by client, s_day;
    """
    update_sql = """
        update operation_client_channel_ticket_daily set order_nums=%s, order_success_nums=%s,
        ticket_num=%s, boat_nums=%s, website_boat_nums=%s, delay_insure_nums=%s, 
        delay_obtain_nums=%s, consumers=%s where s_day=%s and client=%s and channel='合计'
    """
    update_data = DBCli().targetdb_cli.query_all(sql, s_day)
    DBCli().targetdb_cli.batch_insert(update_sql, update_data)


def update_hbgj_channel_client_ticket_h5_daily(days=1):
    """H5客户端渠道机票统计, operation_client_channel_H5_ticket_daily"""
    start_date = DateUtil.get_date_before_days(days * 1)
    end_date = DateUtil.get_date_after_days(1 - days)
    register_users_sql = """
        SELECT DATE_FORMAT(createtime, '%%Y-%%m-%%d') s_day, 
        sum(case when p like 'zhaolian%%' or p like 'huawei%%' or p like 'kaisa%%' 
        or p like 'cgb%%' or p like 'guizhoutong%%' or 
        p like 'yidonghefeixin%%' then 1 else 0 end) H5注册用户
        FROM `phone_user` WHERE `CREATETIME`>%s and CREATETIME<%s
    """

    order_sql = """
            SELECT DATE_FORMAT(o.createtime, '%%Y-%%m-%%d') s_day, left(p,3), 
            sum(case when p like 'zhaolian%%' or p like 'huawei%%' or p like 'kaisa%%' 
            or p like 'cgb%%' or p like 'guizhoutong%%' or 
            p like 'yidonghefeixin%%' then 1 else 0 end) H5订单数
            FROM TICKET_ORDER o 
            WHERE o.CREATETIME>=%s 
            and o.CREATETIME<%s
            and INTFLAG=0 
            AND IFNULL(o.`LINKTYPE`, 0) != 2
            and (p like 'zhaolian%%' or p like 'huawei%%' or p like 'kaisa%%' 
            or p like 'cgb%%' or p like 'guizhoutong%%' or 
            p like 'yidonghefeixin%%')
            GROUP BY s_day,left(p,3);
        """

    success_order_ticket_sql = """
        SELECT DATE_FORMAT(od.createtime, '%%Y-%%m-%%d') s_day,
        left(p, 3),
        count(DISTINCT (case when p like 'zhaolian%%' or p like 'huawei%%' or p like 'kaisa%%' 
        or p like 'cgb%%' or p like 'guizhoutong%%' or 
        p like 'yidonghefeixin%%' then o.ORDERID end)) H5成功订单数
        FROM `TICKET_ORDERDETAIL` od 
        INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        where od.CREATETIME>=%s and od.CREATETIME<%s
        and (p like 'zhaolian%%' or p like 'huawei%%' or p like 'kaisa%%' 
        or p like 'cgb%%' or p like 'guizhoutong%%' or 
        p like 'yidonghefeixin%%')
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
        IFNULL(od.`LINKTYPE`, 0) != 2 and INTFLAG=0 
        GROUP BY s_day, left(p, 3);
        """

    ticket_sql = """
        SELECT DATE_FORMAT(od.createtime, '%%Y-%%m-%%d') s_day, left(p, 3),
        sum(case when p like 'zhaolian%%' or p like 'huawei%%' or p like 'kaisa%%' 
        or p like 'cgb%%' or p like 'guizhoutong%%' or 
        p like 'yidonghefeixin%%' then 1 else 0 end) H5订票数
        FROM `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        where od.CREATETIME>=%s and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) 
        and (p like 'zhaolian%%' or p like 'huawei%%' or p like 'kaisa%%' 
        or p like 'cgb%%' or p like 'guizhoutong%%' or 
        p like 'yidonghefeixin%%')
        AND IFNULL(od.`LINKTYPE`, 0) != 2 and INTFLAG=0 
        GROUP BY s_day, left(p, 3);
        """

    boat_insure_sql = """
        SELECT DATE_FORMAT(od.createtime, '%%Y-%%m-%%d') s_day,left(p, 3),
        count(DISTINCT case when p like 'zhaolian%%' or p like 'huawei%%' or p like 'kaisa%%' 
        or p like 'cgb%%' or p like 'guizhoutong%%' or 
        p like 'yidonghefeixin%%' then i.insureid  end) 
        FROM `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        join INSURE_ORDERDETAIL i on o.ORDERID=i.outorderid
        where od.CREATETIME>=%s and od.CREATETIME<%s 
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
        IFNULL(od.`LINKTYPE`, 0) != 2 and INTFLAG=0
        and (p like 'zhaolian%%' or p like 'huawei%%' or p like 'kaisa%%' 
        or p like 'cgb%%' or p like 'guizhoutong%%' or 
        p like 'yidonghefeixin%%') 
        and i.insurecode in (
        select DISTINCT id from INSURE_DATA where bigtype in (2))
        GROUP BY s_day, left(p, 3)

        """

    web_insure_sql = """
        SELECT DATE_FORMAT(od.createtime, '%%Y-%%m-%%d') s_day,left(p, 3),
        count(DISTINCT(case when p like 'zhaolian%%' or p like 'huawei%%' or p like 'kaisa%%' 
        or p like 'cgb%%' or p like 'guizhoutong%%' or 
        p like 'yidonghefeixin%%' then i.insureid  end)) H5
        FROM `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        join INSURE_ORDERDETAIL i on CONCAT('P',o.ORDERID)=i.OUTORDERID
        where od.CREATETIME>=%s and od.CREATETIME<%s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
        IFNULL(od.`LINKTYPE`, 0) != 2 and INTFLAG=0
        and (p like 'zhaolian%%' or p like 'huawei%%' or p like 'kaisa%%' 
        or p like 'cgb%%' or p like 'guizhoutong%%' or 
        p like 'yidonghefeixin%%')
        and i.insurecode in (
        select DISTINCT id from INSURE_DATA where bigtype in (2)) 
        GROUP BY s_day, left(p, 3);
        """

    delay_insure_sql = """
            SELECT flydate, left(p, 3),
            count(DISTINCT(case when p like 'zhaolian%%' or p like 'huawei%%' or p like 'kaisa%%' 
            or p like 'cgb%%' or p like 'guizhoutong%%' or 
            p like 'yidonghefeixin%%' then o.ORDERID  end)) h5 
            FROM `TICKET_DELAY_CARE` d 
            join TICKET_ORDER o on d.ORDERID=o.ORDERID
            WHERE flydate>=%s and flydate<%s
            and (p like 'zhaolian%%' or p like 'huawei%%' or p like 'kaisa%%' 
            or p like 'cgb%%' or p like 'guizhoutong%%' or 
            p like 'yidonghefeixin%%')
            and state='1' and INTFLAG=0 GROUP BY flydate, left(p, 3);
        """

    delay_accquire_sql = """

            SELECT flydate,left(p, 3),
            count(DISTINCT(case when p like 'zhaolian%%' or p like 'huawei%%' or p like 'kaisa%%' 
            or p like 'cgb%%' or p like 'guizhoutong%%' or 
            p like 'yidonghefeixin%%' then o.ORDERID  end)) h5
            FROM `TICKET_DELAY_CARE` d 
            join TICKET_ORDER o on d.ORDERID=o.ORDERID
            WHERE flydate>=%s and flydate<%s and state='1' 
            and INTFLAG=0 and chargetime<>0 and chargenum!=0 
            and (p like 'zhaolian%%' or p like 'huawei%%' or p like 'kaisa%%' 
            or p like 'cgb%%' or p like 'guizhoutong%%' or 
            p like 'yidonghefeixin%%')
            GROUP BY flydate, left(p, 3);

        """

    consumers_sql = """
            SELECT DATE_FORMAT(od.createtime, '%%Y-%%m-%%d') s_day, left(p, 3),
            count(DISTINCT(case when p like 'zhaolian%%' or p like 'huawei%%' or p like 'kaisa%%' 
            or p like 'cgb%%' or p like 'guizhoutong%%' or 
            p like 'yidonghefeixin%%' then PHONEID  end)) h5
            FROM `TICKET_ORDERDETAIL` od 
            INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
            join PNRSOURCE_CONFIG c on o.PNRSOURCE=c.PNRSOURCE
            where od.CREATETIME>=%s and od.CREATETIME<%s
            and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) 
            AND IFNULL(od.`LINKTYPE`, 0) != 2 
            and INTFLAG=0 and FIRSTPAY=1
            and (p like 'zhaolian%%' or p like 'huawei%%' or p like 'kaisa%%' 
            or p like 'cgb%%' or p like 'guizhoutong%%' or 
            p like 'yidonghefeixin%%')
            GROUP BY s_day, left(p, 3);

        """
    dto = [start_date, end_date]

    order_data = DBCli().sourcedb_cli.query_all(order_sql, dto)
    insert_order_sql = """
        insert into operation_client_channel_H5_ticket_daily
        (s_day, client, pn_resource, register_users, order_nums,
        order_success_nums, ticket_num, boat_nums, website_boat_nums,
        delay_insure_nums, delay_obtain_nums, consumers, create_time, update_time)
        values (%s, %s, %s, 0, %s, 0, 0, 0, 0, 0, 0, 0, now(), now())
    """
    for o in order_data:
        s_day, pn, num = o
        DBCli().targetdb_cli.insert(insert_order_sql, [s_day, "H5", pn, num])

    success_order_data = DBCli().sourcedb_cli.query_all(success_order_ticket_sql, dto)
    update_success_sql = """
        update operation_client_channel_H5_ticket_daily set order_success_nums=%s
        where s_day=%s and pn_resource=%s
    """
    for s in success_order_data:
        s_day, pn, num = s
        DBCli().targetdb_cli.insert(update_success_sql, [num, s_day, pn])

    ticket_data = DBCli().sourcedb_cli.query_all(ticket_sql, dto)
    update_ticket_sql = """
        update operation_client_channel_H5_ticket_daily set ticket_num=%s
        where s_day=%s and pn_resource=%s
    """

    for t in ticket_data:
        s_day, pn, num = t
        DBCli().targetdb_cli.insert(update_ticket_sql, [num, s_day, pn])

    boat_insure_data = DBCli().sourcedb_cli.query_all(boat_insure_sql, dto)
    update_boat_sql = """
        update operation_client_channel_H5_ticket_daily set boat_nums=%s
        where s_day=%s and pn_resource=%s
    """
    for b in boat_insure_data:
        s_day, pn, num = b
        DBCli().targetdb_cli.insert(update_boat_sql, [num, s_day, pn])

    web_insure_data = DBCli().sourcedb_cli.query_all(web_insure_sql, dto)
    update_web_insure_sql = """
        update operation_client_channel_H5_ticket_daily set website_boat_nums=%s
        where s_day=%s and pn_resource=%s
    """
    for w in web_insure_data:
        s_day, pn, num = w
        DBCli().targetdb_cli.insert(update_web_insure_sql, [num, s_day, pn])

    delay_insure_data = DBCli().sourcedb_cli.query_all(delay_insure_sql, dto)
    update_delay_insure_sql = """
        update operation_client_channel_H5_ticket_daily set delay_insure_nums=%s
        where s_day=%s and pn_resource=%s
    """
    for d in delay_insure_data:
        s_day, pn, num = d
        DBCli().targetdb_cli.insert(update_delay_insure_sql, [num, s_day, pn])

    delay_accquire_data = DBCli().sourcedb_cli.query_all(delay_accquire_sql, dto)
    update_delay_obtain_sql = """
        update operation_client_channel_H5_ticket_daily set delay_obtain_nums=%s
        where s_day=%s and pn_resource=%s
    """
    for dc in delay_accquire_data:
        s_day, pn, num = dc
        DBCli().targetdb_cli.insert(update_delay_obtain_sql, [num, s_day, pn])

    consumers_data = DBCli().sourcedb_cli.query_all(consumers_sql, dto)
    update_consumers_sql = """
        update operation_client_channel_H5_ticket_daily set consumers=%s
        where s_day=%s and pn_resource=%s
    """
    for u in consumers_data:
        s_day, pn, num = u
        DBCli().targetdb_cli.insert(update_consumers_sql, [num, s_day, pn])


    update_hj_sql = """
        select sum(order_nums), sum(order_success_nums),
        sum(ticket_num), sum(boat_nums), sum(website_boat_nums),
        sum(delay_insure_nums), sum(delay_obtain_nums), sum(consumers)
        from operation_client_channel_H5_ticket_daily
        where s_day=%s and pn_resource !="合计"
        group by client, s_day;
    """
    hj_data = DBCli().targetdb_cli.query_one(update_hj_sql, [start_date])
    register_users = DBCli().sourcedb_cli.query_one(register_users_sql, dto)
    insert_reg_sql = """
        insert into operation_client_channel_H5_ticket_daily
        (s_day, client, pn_resource, register_users, order_nums, 
        order_success_nums, ticket_num, boat_nums, website_boat_nums, 
        delay_insure_nums, delay_obtain_nums, consumers, create_time, update_time)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
    """
    insert_data = [start_date, "H5", "合计"] + [register_users[1]] + list(hj_data)
    DBCli().targetdb_cli.insert(insert_reg_sql, insert_data)


if __name__ == '__main__':
    # i = 1
    # while i <= 4:
    #     update_hbgj_channel_client_ticket_daily(i)
    #     i += 1
    i = 5
    while i <= 12:
        update_hbgj_channel_client_ticket_h5_daily(i)
        i += 1