# -*- coding: utf-8 -*-
import requests
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from collections import defaultdict
import core


def cal_uv_pv(app, event, result_dict, start_date, end_date):

    gt_ios_pv = core.request_pv(app, start_date, end_date, event, 'day')
    gt_ios_uv = core.request_uv(app, start_date, end_date, event, 'day')

    for gt in gt_ios_uv:
        result_dict[gt['day']].append(gt['users'])

    for gt in gt_ios_pv:
        result_dict[gt['day']].append(gt['sessions_per_event'])


def hb_gt_travel_daily(days=1):
    """高铁行程localytics, hb_gt_travel_pv_uv_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 1), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_before_days(days - 1), '%Y-%m-%d')
    result_dict = defaultdict(list)
    train_detail_event = ['ios.train.detail.open', 'android.train.detail.open']
    for event in train_detail_event:
        cal_uv_pv('gt', event, result_dict, start_date, end_date)

    train_info_event = ['ios.train.info.open', 'android.train.info.open']
    for event in train_info_event:
        cal_uv_pv('gt', event, result_dict, start_date, end_date)

    train_carriage_event = ['ios.weex.train.carriage.open', 'android.weex.train.carriage.open']
    for event in train_carriage_event:
        cal_uv_pv('gt', event, result_dict, start_date, end_date)

    train_map_event = ['ios.train.map.open', 'android.train.map.open']
    for event in train_map_event:
        cal_uv_pv('gt', event, result_dict, start_date, end_date)

    train_message_event = ['ios.train.message.open', 'android.train.message.open']
    for event in train_message_event:
        cal_uv_pv('gt', event, result_dict, start_date, end_date)

    train_message_event = ['ios.flight.main.open', 'android.flight.main.open']
    for event in train_message_event:
        cal_uv_pv('hb', event, result_dict, start_date, end_date)

    insert_data = []
    for k, v in result_dict.items():
        v.insert(0, k)
        insert_data.append(v)

    insert_sql = """
        insert into hb_gt_travel_pv_uv_daily (s_day, `ios.train.detail.open.uv`, `ios.train.detail.open.pv`, `android.train.detail.open.uv`
            ,`android.train.detail.open.pv`, `ios.train.info.open.uv`, `ios.train.info.open.pv`, `android.train.info.uv`, `android.train.info.pv`
            ,`ios.weex.train.carriage.open.uv`, `ios.weex.train.carriage.open.pv`, `android.weex.train.carriage.open.uv`,`android.weex.train.carriage.open.pv`
            ,`ios.train.map.open.uv`, `ios.train.map.open.pv`, `android.train.map.open.uv`, `android.train.map.open.pv`
            ,`ios.train.message.open.uv`, `ios.train.message.open.pv`, `android.train.message.open.uv`, `android.train.message.open.pv`
            ,create_time, update_time)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
    """
    DBCli().targetdb_cli.batch_insert(insert_sql, insert_data)


def station_pv_uv(days=1):
    """高铁大屏localytics, gt_station_pv_uv_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 1), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_before_days(days - 1), '%Y-%m-%d')
    result_dict = defaultdict(list)
    train_detail_event = ['ios.weex.station.screen.main.open', 'android.weex.station.screen.main.open']
    for event in train_detail_event:
        cal_uv_pv('gt', event, result_dict, start_date, end_date)

    train_detail_event = ['ios.weex.station.screen.info.open', 'android.weex.station.screen.info.open']
    for event in train_detail_event:
        cal_uv_pv('gt', event, result_dict, start_date, end_date)

    train_detail_event = ['ios.weex.station.food.list.open', 'android.weex.station.food.list.open']
    for event in train_detail_event:
        cal_uv_pv('gt', event, result_dict, start_date, end_date)

    train_detail_event = ['ios.weex.station.food.detail.open', 'android.weex.station.food.detail.open']
    for event in train_detail_event:
        cal_uv_pv('gt', event, result_dict, start_date, end_date)

    insert_sql = """
        insert into gt_station_pv_uv_daily (
            s_day,
            `ios.weex.station.screen.main.open.uv`,
            `ios.weex.station.screen.main.open.pv`,
            `android.weex.station.screen.main.open.uv`,
            `android.weex.station.screen.main.open.pv`,
            `ios.weex.station.screen.info.uv`,
            `ios.weex.station.screen.info.pv`,
            `android.weex.station.screen.info.uv`,
            `android.weex.station.screen.info.pv`,
            `ios.weex.station.food.list.open.uv`,
            `ios.weex.station.food.list.open.pv`,
            `android.weex.station.food.list.open.uv`,
            `android.weex.station.food.list.open.pv`,
            `ios.weex.station.food.detail.open.uv`,
            `ios.weex.station.food.detail.open.pv`,
            `android.weex.station.food.detail.open.uv`,
            `android.weex.station.food.detail.open.pv`,
            `create_time`,
            `update_time`
        ) values (%s, %s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, now(), now())
            
    """

    insert_data = []
    for k, v in result_dict.items():
        v.insert(0, k)
        insert_data.append(v)
    DBCli().targetdb_cli.batch_insert(insert_sql, insert_data)


if __name__ == '__main__':
    station_pv_uv(1)
