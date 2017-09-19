# -*- coding: utf-8 -*-
from dbClient.dateutil import DateUtil
from dbClient.db_client import DBCli
import datetime
from collections import defaultdict
from mako.template import Template
from mako.lookup import TemplateLookup
import os


def mako_render(data, mako_file):
    directories = os.path.join(os.path.dirname(__file__), '')
    mako_lookup = TemplateLookup(directories=directories, input_encoding='utf-8',
                                 output_encoding='utf-8',
                                 default_filters=['decode.utf_8'])
    mako_template = Template('<%include file="{}"/>'.format(mako_file),
                             lookup=mako_lookup, input_encoding='utf-8',
                             default_filters=['decode.utf_8'],
                             output_encoding='utf-8')
    content = mako_template.render(**data)
    return content


def monitor_hb_channel_ticket():
    now_hour_sql = """
        SELECT c.NAME as 渠道,
        count(*) as 票量
        FROM `TICKET_ORDERDETAIL` od INNER JOIN `TICKET_ORDER` o on od.ORDERID=o.ORDERID
        join `PNRSOURCE_CONFIG` c on o.PNRSOURCE=c.PNRSOURCE
        where
        od.CREATETIME >= %s
        and od.CREATETIME <= %s
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) AND
        IFNULL(od.`LINKTYPE`, 0) != 2 GROUP BY DATE_FORMAT(od.CREATETIME, '%%Y-%%m-%%d'), o.PNRSOURCE;
    """
    start_date = DateUtil.get_date_before_days(1)
    end_date = DateUtil.get_date_after_days(0)
    dto = [start_date, end_date]
    # rows_heads = [u'渠道']
    channel_list = []

    head = map(lambda x: DateUtil.add_days(start_date, -x), xrange(1, 4))
    result_ticket_num = {}
    for index, start in enumerate(head):
        end_date = DateUtil.add_days(start, 1)
        query_data = DBCli().sourcedb_cli.queryAll(now_hour_sql, [start, end_date])

        for data in query_data:
            pn_name, ticket_num = data
            if index == 0:
                result_ticket_num[pn_name] = [0] * len(head)
                result_ticket_num[pn_name][index] = ticket_num
                channel_list.append(pn_name)
            else:
                if pn_name not in channel_list:
                    continue
                if result_ticket_num.get(pn_name, None) is None:
                    result_ticket_num[pn_name] = [0] * len(head)
                else:
                    result_ticket_num[pn_name][index] = ticket_num

    rows = [''] + head + ['前7天的平均票数', '前7天最少票数', ]
    rows_headers = [u'渠道'] + [u'票数'] * len(head)

    rows_data = []
    for k, v in sorted(result_ticket_num.items(), key=lambda a: a[1], reverse=True):
        avg_num = float(v[1:]/len(v[1:]))
        min_num = min(v[1:])
        rows_data.append([k] + v + [avg_num, min_num])
    print rows_data
    data = {
        'rows': rows,
        'rows_headers': rows_headers,
        'rows_data': rows_data
    }
    text = mako_render(data, 'email.txt')
    from dbClient import utils
    utils.sendMail('762575190@qq.com', text, '航班订票渠道数据')

    # for d in query_data:
    #     print d


if __name__ == '__main__':
    monitor_hb_channel_ticket()
