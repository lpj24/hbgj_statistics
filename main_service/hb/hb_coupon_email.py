# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import requests
from mako.template import Template
from mako.lookup import TemplateLookup
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


def send_hb_delay_email(days=0):
    # 延误宝
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)), '%Y-%m-%d')
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(3), '%Y-%m-%d')

    dto = [start_date, end_date]
    delay_insure_sql = """
        SELECT DATE_FORMAT(create_time, '%%Y-%%m-%%d') s_day,
        count(CASE WHEN entry=1 then pack_id end ) 跟随订单购买,
        count(CASE WHEN entry in (2,3,4) then pack_id end ) 单独购买,
        count(CASE WHEN entry=5 then pack_id end ) 非航班管家用户购买
        FROM `ticket_delay_pack` WHERE create_time>=%s and create_time<%s GROUP BY s_day;

    """

    delay_num_amount_sql = """
        SELECT LEFT(qbr.create_time, 10) as dayDate, COUNT(DISTINCT qbr.pack_id) AS countNum, SUM(qbr.bonus_amount) as sumAmount
        FROM (
        SELECT DISTINCT q.receive_number, q.pack_id, q.bonus_amount, q.receive_status, q.create_time
        FROM quota_bonus_receive q where LEFT(q.create_time, 10)=%s
        GROUP BY q.pack_id, q.receive_number
        ) AS qbr
        GROUP BY dayDate
        ORDER BY dayDate DESC 
    """
    delay_data = DBCli().delay_insure_cli.query_all(delay_insure_sql, dto)
    delay_email_list = [d for d in delay_data]
    render_data = {
        'rows': ['日期', '跟随订单购买', '单独购买', '非航班管家用户购买'],
        'rows_data': delay_email_list
    }
    delay_msg_text = "<strong>延误宝销售量</strong><br/><br/>"
    delay_msg_text += mako_render(render_data, 'sign_template.txt')

    delay_num_amount_data = DBCli().delay_insure_cli.query_one(delay_num_amount_sql, [start_date])
    render_data = {
        'rows': ['日期', '赔付个数', '总金额'],
        'rows_data': [delay_num_amount_data]
    }
    delay_msg_text += "<br/><br/><strong>延误宝赔付个数和总金额</strong><br/><br/>"
    delay_msg_text += mako_render(render_data, 'sign_template.txt')
    return delay_msg_text


def send_hb_coupon_email(days=0):
    end_date = DateUtil.date2str(DateUtil.get_date_after_days(1 - int(days)), '%Y-%m-%d')
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days), '%Y-%m-%d')

    dto = [start_date, end_date]
    coupon_add_sql = """
        SELECT left(c.createtime,10),
        c.coupon_id,
        l.coupon_name,
        count(1) 
        FROM account.coupon c JOIN account.coupon_list l on l.id=c.coupon_id
        WHERE l.costid=1 and c.createtime 
        BETWEEN %s and %s GROUP BY c.coupon_id limit 100;
    """

    coupon_use_sql = """
        SELECT left(c.updatetime ,10),
        c.coupon_id,
        l.coupon_name,
        count(1) FROM account.coupon c JOIN account.coupon_list l on l.id=c.coupon_id
        WHERE l.costid=1 and c.updatetime 
        BETWEEN %s and %s and used=1 GROUP BY c.coupon_id limit 100;

    """

    coupon_add_data = DBCli().sourcedb_cli.query_all(coupon_add_sql, dto)
    coupon_use_data = DBCli().sourcedb_cli.query_all(coupon_use_sql, dto)

    coupon_add_email_list = []
    coupon_use_email_list = []
    for data in coupon_add_data:
        coupon_add_email_list.append([data[0], data[1], data[2]])

    render_data = {
        'rows': ['日期', 'coupon_id', 'coupon_name', '数量'],
        'rows_data': coupon_add_email_list
    }
    sign_msg_text += "<strong>优惠券新增量</strong><br/><br/>"
    sign_msg_text += mako_render(render_data, 'sign_template.txt')

    for data in coupon_use_data:
        coupon_use_email_list.append([data[0], data[1], data[2]])

    render_data = {
        'rows': ['coupon_id', 'coupon_name', '数量'],
        'rows_data': coupon_use_email_list
    }
    sign_msg_text += "<br/><br/><strong>优惠券使用量</strong><br/><br/>"
    sign_msg_text += mako_render(render_data, 'sign_template.txt')

    # 优惠券使用率
    # 使用量
    use_coupon_rate_email_list = []
    total_use_sql = """
        SELECT CONCAT(c.coupon_id, ":", l.coupon_name) id_name,count(*) 
        FROM account.coupon c JOIN account.coupon_list l on l.id=c.coupon_id
        WHERE l.costid=1  and used=1 GROUP BY id_name;
    """

    total_provide_sql = """
        SELECT CONCAT(c.coupon_id, ":", l.coupon_name) id_name,count(*) 
        FROM account.coupon c JOIN account.coupon_list l on l.id=c.coupon_id
        WHERE l.costid=1  GROUP BY id_name;
    """

    total_use_data = dict(DBCli().sourcedb_cli.query_all(total_use_sql))
    total_provide_data = dict(DBCli().sourcedb_cli.query_all(total_provide_sql))

    for k, v in total_provide_data.items():
        coupon_id, coupon_name = k.split(':')
        coupon_use = total_use_data[k]
        use_rate = float(coupon_use)/float(v)
        use_coupon_rate_email_list.append([coupon_id, coupon_name, coupon_use, v, use_rate])

    render_data = {
        'rows': ['coupon_id', 'coupon_name', u'优惠券使用量', u'优惠券发放数量', u'优惠券使用率'],
        'rows_data': use_coupon_rate_email_list
    }
    sign_msg_text += "<br/><br/><strong>优惠券使用率</strong><br/><br/>"
    sign_msg_text += mako_render(render_data, 'sign_template.txt')
    return sign_msg_text


def send_hb_sign_email():
    sign_url = 'http://jt.rsscc.com/hbgjact/hbgjact/sign/signStatistics.action'
    sign_data = requests.get(sign_url)
    sign_detail_data = sign_data.json()['data']

    cntBySignDays = []
    lastweekSigncntPerday = []
    cntByRedpacketCnt = []
    for data in sign_detail_data['lastweekSigncntPerday']:
        lastweekSigncntPerday.append([data['signDate'], data['cnt']])

    print sign_detail_data['lastweekSignTotal']

    for data in sign_detail_data['cntBySignDays']:
        cntBySignDays.append([data['signDays'], data['cnt']])

    for data in sign_detail_data['cntByRedpacketCnt']:
        cntByRedpacketCnt.append([data['redpacketCnt'], data['cnt']])

    render_data = {
        'rows': [u'上周日期', u'人数'],
        'rows_data': lastweekSigncntPerday
    }
    sign_msg_text += "<strong>上周每天签到人数</strong><br/><br/>"
    sign_msg_text += mako_render(render_data, 'sign_template.txt')
    sign_msg_text += '<br/>' + '<p>上周签到总人数: ' + str(sign_detail_data['lastweekSignTotal']) + '</p><br/><br/>'

    render_data = {
        'rows': [u'参与天数', u'人数'],
        'rows_data': cntBySignDays
    }

    sign_msg_text += "<strong>按参与天数汇总</strong><br/><br/>"
    sign_msg_text += mako_render(render_data, 'sign_template.txt') + "<br/>"

    render_data = {
        'rows': [u'红包数量', u'人数'],
        'rows_data': cntByRedpacketCnt
    }

    sign_msg_text += "<strong>按获得红包总数汇总</strong><br/><br/>"
    sign_msg_text += mako_render(render_data, 'sign_template.txt')
    return sign_msg_text


if __name__ == '__main__':
    global sign_msg_text
    from dbClient.utils import sendMail
    sign_msg_text = send_hb_coupon_email(1)
    sign_msg_text += send_hb_delay_email(1)
    subject = DateUtil.date2str(DateUtil.get_date_before_days(1), '%Y-%m-%d') + u' 航班管家优惠券与延误宝统计'
    sendMail('762575190@qq.com', sign_msg_text, "")
    # print send_hb_delay_email(1)