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
                v.insert(0, 1)
                v.insert(0, "直销")
                v.insert(0, "航班管家")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "zy":
                v.insert(0, 2)
                v.insert(0, "自营")
                v.insert(0, "航班管家")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "hzf":
                v.insert(0, 3)
                v.insert(0, "合作方")
                v.insert(0, "航班管家")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "gwdg":
                v.insert(0, 4)
                v.insert(0, "官网代购")
                v.insert(0, "航班管家")
                v.insert(0, start_date)
                last_insert_data.append(v)
        elif k[0] == "gt":
            if k[1] == "zx":
                v.insert(0, 1)
                v.insert(0, "直销")
                v.insert(0, "高铁管家")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "zy":
                v.insert(0, 2)
                v.insert(0, "自营")
                v.insert(0, "高铁管家")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "hzf":
                v.insert(0, 3)
                v.insert(0, "合作方")
                v.insert(0, "高铁管家")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "gwdg":
                v.insert(0, 4)
                v.insert(0, "官网代购")
                v.insert(0, "高铁管家")
                v.insert(0, start_date)
                last_insert_data.append(v)
        elif k[0] == "wx":
            if k[1] == "zx":
                v.insert(0, 1)
                v.insert(0, "直销")
                v.insert(0, "微信")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "zy":
                v.insert(0, 2)
                v.insert(0, "自营")
                v.insert(0, "微信")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "hzf":
                v.insert(0, 3)
                v.insert(0, "合作方")
                v.insert(0, "微信")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "gwdg":
                v.insert(0, 4)
                v.insert(0, "官网代购")
                v.insert(0, "微信")
                v.insert(0, start_date)
                last_insert_data.append(v)
        elif k[0] == "gtwx":
            if k[1] == "zx":
                v.insert(0, 1)
                v.insert(0, "直销")
                v.insert(0, "高铁微信小程序")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "zy":
                v.insert(0, 2)
                v.insert(0, "自营")
                v.insert(0, "高铁微信小程序")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "hzf":
                v.insert(0, 3)
                v.insert(0, "合作方")
                v.insert(0, "高铁微信小程序")
                v.insert(0, start_date)
                last_insert_data.append(v)
            elif k[1] == "gwdg":
                v.insert(0, 4)
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

    new_last_insert_data.append([start_date, "航班管家", "合计", 0, reg_users_dict[u"hb-注册用户"], 0, 0, 0, 0, 0, 0, 0, 0])
    new_last_insert_data.append([start_date, "高铁管家", "合计", 0, reg_users_dict[u"gt-注册用户"], 0, 0, 0, 0, 0, 0, 0, 0])
    new_last_insert_data.append([start_date, "微信", "合计", 0, reg_users_dict[u"wx-注册用户"], 0, 0, 0, 0, 0, 0, 0, 0])
    new_last_insert_data.append([start_date, "高铁微信小程序", "合计", 0, reg_users_dict[u"gtwx-注册用户"], 0, 0, 0, 0, 0, 0, 0, 0])

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


if __name__ == '__main__':
    i = 1
    while i <= 4:
        start_date = DateUtil.get_date_before_days(i * 1)
        end_date = DateUtil.get_date_after_days(1 - i)
        update_client_channel_hj(start_date)
        i += 1