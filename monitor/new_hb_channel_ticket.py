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
    start_date = DateUtil.get_date_before_days(10)
    channel_list = []

    head = map(lambda x: DateUtil.add_days(start_date, -x), xrange(1, 7))
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

    rows = [''] + head + ['较5天的平均票数', '前5天最少票数']
    rows_headers = [u'渠道'] + [u'票数'] * len(head) + [u'百分比'] * 2

    rows_data = []
    for k, v in sorted(result_ticket_num.items(), key=lambda a: a[1], reverse=True):
        avg_num = float(sum(v[1:])/len(v[1:]))
        min_num = min(v[1:])
        today_num = v[0]
        avg_num_per = 0 if avg_num == 0 else (float(today_num) - avg_num)/avg_num * 100
        min_num_per = 0 if min_num == 0 else (float(today_num) - min_num) / min_num * 100
        if avg_num_per < -20 and min_num_per < -20:
            avg_num_per = '<font color="#FF0000">{0:.2f}%</font> (平均数: {1})'.format(avg_num_per, avg_num)
            min_num_per = '<font color="#FF0000">{0:.2f}%</font> (最小票数: {1})'.format(min_num_per, min_num)
        else:
            avg_num_per = '{0:.2f}% (平均数: {1})'.format(avg_num_per, avg_num)
            min_num_per = '{0:.2f}% (最小票数: {1})'.format(min_num_per, min_num)

        rows_data.append([k] + v + [avg_num_per, min_num_per])

    data = {
        'rows': rows,
        'rows_headers': rows_headers,
        'rows_data': rows_data
    }
    text = mako_render(data, 'email.txt')
    from dbClient import utils
    utils.sendMail('762575190@qq.com', text, '航班订票渠道数据')


def get_median(data):
    data.sort()
    half = len(data) // 2
    return float(data[half] + data[~half])/2

if __name__ == '__main__':
    monitor_hb_channel_ticket()