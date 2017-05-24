# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from collections import defaultdict
from dbClient import utils


def hbgj_user(days=0):
    """通过邮件发送航班数据, send_sms"""
    start_date = DateUtil.get_date_before_days(days)
    end_date = DateUtil.get_date_after_days(1 - int(days))
    # 活跃用户
    active_users = """
        select
        DATE_FORMAT(createtime, '%%Y-%%m-%%d') s_day, source, count(DISTINCT userid) active_users
        from ACTIVE_USER_LOG
        where
        createtime >= %s
        and createtime < %s
        and p like '%%hbgj%%'
        group by s_day, source
        order by active_users desc
    """

    # new_users
    new_users = """
        select DATE_FORMAT(USER_CREATEDATE,'%%Y-%%m-%%d') s_day,
        USER_CHANNEL,
        count(*) new_users
        from HBZJ_USER
        where USER_CREATEDATE>=%s
        and USER_CREATEDATE<%s
        group by s_day,USER_CHANNEL
        ORDER BY new_users desc
    """

    other_sql = """
    select trp.s_day,trp.source, count(trp.ORDERID) order_num, SUM(trp.TICKET_COUNT) TICKET_NUM,
    sum(trp.PAYPRICE) price
    from (
    SELECT DATE_FORMAT(TR.createtime,'%%Y-%%m-%%d') s_day,
    SUBSTRING(p, 1, LOCATE(",", p)-1) source, PAYPRICE, ORDERID,
    (SELECT COUNT(1) FROM TICKET_ORDERDETAIL TOD2 WHERE TOD2.ORDERID=TR.ORDERID) TICKET_COUNT
    FROM TICKET_ORDER TR
    where TR.ORDERSTATUE not in (2,12,21,51,75)
    and TR.createtime>=%s AND TR.createtime<%s
    ) trp
    GROUP BY trp.s_day, trp.source
    order by order_num desc
    """

    other_sum = """
            select trp.s_day,count(trp.ORDERID) order_num, SUM(trp.TICKET_COUNT) ticket_sum,
            sum(trp.PAYPRICE) price_sum
            from (
            SELECT DATE_FORMAT(TR.createtime,'%%Y-%%m-%%d') s_day,
            PAYPRICE, ORDERID,
            (SELECT COUNT(1) FROM TICKET_ORDERDETAIL TOD2 WHERE TOD2.ORDERID=TR.ORDERID) TICKET_COUNT
            FROM TICKET_ORDER TR
            where TR.ORDERSTATUE not in (2,12,21,51,75)
            and TR.createtime>=%s AND TR.createtime<%s
            ) trp
            GROUP BY trp.s_day
            order by order_num desc

    """
    # insert_sql = """
    #     insert into hbgj_users_info (s_day, source, new_users, active_users, ticket_order, ticket_count,ticket_amount)
    #     values (%s, %s, %s ,%s ,%s ,%s, %s)
    # """

    while start_date < end_date:
        last_data = []
        query_start_date = DateUtil.date2str(start_date, '%Y-%m-%d')
        query_end_date = DateUtil.date2str(DateUtil.add_days(start_date, 1), '%Y-%m-%d')

        other_start_date = DateUtil.date2str(start_date)
        other_end_date = DateUtil.date2str(DateUtil.add_days(start_date, 1))

        insert_data = defaultdict(list)

        new_users_dto = [query_start_date, query_end_date]
        new_users_data = DBCli().apibase_cli.queryAll(new_users, new_users_dto)

        new_users_sum = 0
        active_users_sum = 0

        for user_data in new_users_data:
            new_users_sum += user_data[2]
            insert_data[user_data[1]].append(user_data[2])

        active_users_dto = [other_start_date, other_end_date]
        active_users_data = DBCli().apibase_cli.queryAll(active_users, active_users_dto)

        for active_data in active_users_data:
            active_users_sum += active_data[2]
            if not insert_data.has_key(active_data[1]):
                insert_data[active_data[1]].append(0)
                insert_data[active_data[1]].append(active_data[2])
            else:
                insert_data[active_data[1]].append(active_data[2])

        for k, v in insert_data.items():
            if len(v) == 1:
                insert_data[k].append(0)

        other_dto = [other_start_date, other_end_date]
        other_data = DBCli().sourcedb_cli.queryAll(other_sql, other_dto)

        for other_data in other_data:
            if not insert_data.has_key(other_data[1]):
                for i in xrange(2):
                    insert_data[other_data[1]].append(0)

            for i in xrange(3):
                insert_data[other_data[1]].append(other_data[2+i])

        for k, v in insert_data.items():
            if len(v) == 2:
                for i in xrange(3):
                    insert_data[k].append(0)
        #
        for k, v in insert_data.items():
            result_data = []
            if k:
                result_data.append(query_start_date)
                result_data.append(k)
                for num in v:
                    result_data.append(num)

                last_data.append(tuple(result_data))

        last_data.sort(cmp=lambda x, y: cmp(x[2], y[2]), reverse=True)

        sum_data = DBCli().sourcedb_cli.queryOne(other_sum, other_dto)

        subject = DateUtil.date2str(start_date, "%Y-%m-%d") + " 航班管家用户统计"
        msgtext = "<table> <tr>" \
                  "<th align='left'>渠道</th>" \
                  "<th align='left'>新用户</th>" \
                  "<th align='left'>活跃用户</th>" \
                  "<th align='left'>订单数</th>" \
                  "<th align='left'>机票数</th>" \
                  "<th align='left'>订单金额</th>" \
                  "</tr>" \
                  "<tr>" \
                  "<td>总计</td>" \
                  "<td>"+str(new_users_sum)+"</td>" \
                  "<td>"+str(active_users_sum)+"</td>" \
                  "<td>"+str(sum_data[1])+"</td>" \
                  "<td>"+str(sum_data[2])+"</td>" \
                  "<td>"+str(sum_data[3])+"</td>" \
                  "</tr>"
        # msgtext = msgtext + "<table border='1'>"
        for i in last_data:
            msg = "".join(["<tr>",
                           "<td width='15%'>",
                           str(i[1]),
                           "</td>",
                           "<td width='15%'>",
                           str(i[2]),
                           "</td>",
                           "<td width='15%'>",
                           str(i[3]),
                           "</td>",
                           "<td width='15%'>",
                           str(i[4]),
                           "</td>",
                           "<td width='15%'>",
                           str(i[5]),
                           "</td>",
                           "<td width='15%'>",
                           str(i[6]),
                           "</td>",
                           "</tr>"])
            msgtext += msg
        msgtext += "</table>"
        utils.sendMail('lipenju24@163.com', msgtext, subject)
        utils.sendMail('zhangchao_notice@sina.com', msgtext, subject)
        utils.sendMail('dingqq@133.cn', msgtext, subject)
        utils.sendMail('liangyjy@133.cn', msgtext, subject)
        utils.sendMail('liyang@133.cn', msgtext, subject)
        utils.sendMail('hongb@133.cn', msgtext, subject)
        utils.sendMail('zhangchao@133.cn', msgtext, subject)
        start_date = DateUtil.add_days(start_date, 1)
    return __file__

if __name__ == "__main__":
    hbgj_user(2)