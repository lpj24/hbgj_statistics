# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from collections import defaultdict
from dbClient import utils
from mako.template import Template
from mako.lookup import TemplateLookup
import logging
import os


def mako_render(data, mako_file):
    directories = os.path.join(os.path.dirname(__file__), 'email')
    mako_lookup = TemplateLookup(directories=directories, input_encoding='utf-8',
                                 output_encoding='utf-8',
                                 default_filters=['decode.utf_8'])
    mako_template = Template('<%include file="{}"/>'.format(mako_file),
                             lookup=mako_lookup, input_encoding='utf-8',
                             default_filters=['decode.utf_8'],
                             output_encoding='utf-8')
    content = mako_template.render(**data)
    return content


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
                result_data.append(k)
                for num in v:
                    result_data.append(num)

                last_data.append(tuple(result_data))

        last_data.sort(cmp=lambda x, y: cmp(x[2], y[2]), reverse=True)

        sum_data = DBCli().sourcedb_cli.queryOne(other_sum, other_dto)

        subject = DateUtil.date2str(start_date, "%Y-%m-%d") + u' 航班管家用户统计'
        rows = ['渠道', '新用户', '活跃用户', '订单数', '机票数', '订单金额']
        rows_headers = ['总计', str(new_users_sum), str(active_users_sum), str(sum_data[1]),
                        str(sum_data[2]), str(sum_data[3])]
        data = {
            'rows': rows,
            'rows_headers': rows_headers,
            'rows_data': last_data
        }
        try:
            msgText = mako_render(data, 'email_template.txt')
            utils.sendMail('762575190@qq.com', msgText, subject)
            utils.sendMail('zhangchao_notice@sina.com', msgText, subject)
            utils.sendMail('dingqq@133.cn', msgText, subject)
            utils.sendMail('liangyjy@133.cn', msgText, subject)
            utils.sendMail('liyang@133.cn', msgText, subject)
            utils.sendMail('hongb@133.cn', msgText, subject)
            utils.sendMail('zhangchao@133.cn', msgText, subject)
        except Exception as e:
            logging.warning('hbgj send email error ' + str(e.message))
        start_date = DateUtil.add_days(start_date, 1)
    return __file__

if __name__ == "__main__":
    hbgj_user(1)
    # import os
    # print os.path.dirname(__file__)
    # print os.path.join(os.path.dirname(__file__), 'email')