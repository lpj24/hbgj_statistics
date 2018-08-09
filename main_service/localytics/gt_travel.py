# -*- coding: utf-8 -*-
import requests
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
from collections import defaultdict
import core


def cal_uv_pv(app, event, result_dict, start_date, end_date):

    gt_ios_pv = core.request_pv(app, start_date, end_date, event, 'day', metrics='occurrences')
    gt_ios_uv = core.request_uv(app, start_date, end_date, event, 'day')
    diff_days = DateUtil.minus_days(start_date, end_date) + 1

    gt_ios_pv = {pv['day']: pv['occurrences'] for pv in gt_ios_pv}
    gt_ios_uv = {uv['day']: uv['users'] for uv in gt_ios_uv}

    for i in xrange(diff_days):
        try:
            start_day = DateUtil.date2str(DateUtil.add_days(DateUtil.str2date(start_date, '%Y-%m-%d'), i), '%Y-%m-%d')

            result_dict[start_day].append(gt_ios_uv.get(start_day, 0))
            result_dict[start_day].append(gt_ios_pv.get(start_day, 0))
        except IndexError:
            continue

    # for gt in gt_ios_uv:
    #     result_dict[gt['day']].append(gt['users'])

    # for gt in gt_ios_pv:
    #     result_dict[gt['day']].append(gt['sessions_per_event'])


def hb_gt_travel_daily(days=1):
    """高铁行程localytics, hb_gt_travel_pv_uv_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 219), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_before_days(days*1), '%Y-%m-%d')
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

    # train_message_event = ['ios.flight.main.open', 'android.flight.main.open']
    # for event in train_message_event:
    #     cal_uv_pv('hb', event, result_dict, start_date, end_date)

    insert_data = []
    for k, v in result_dict.items():
        v.insert(0, k)
        insert_data.append(v)

    insert_sql = """
        insert into hb_gt_travel_pv_uv_daily (s_day, `ios_train_detail_open_uv`, `ios_train_detail_open_pv`, `android_train_detail_open_uv`
            ,`android_train_detail_open_pv`, `ios_train_info_open_uv`, `ios_train_info_open_pv`, `android_train_info_uv`, `android_train_info_pv`
            ,`ios_weex_train_carriage_open_uv`, `ios_weex_train_carriage_open_pv`, `android_weex_train_carriage_open_uv`,`android_weex_train_carriage_open_pv`
            ,`ios_train_map_open_uv`, `ios_train_map_open_pv`, `android_train_map_open_uv`, `android_train_map_open_pv`
            ,`ios_train_message_open_uv`, `ios_train_message_open_pv`, `android_train_message_open_uv`, `android_train_message_open_pv`
            ,create_time, update_time)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())

    """
    DBCli().targetdb_cli.batch_insert(insert_sql, insert_data)


def station_pv_uv(days=1):
    """高铁大屏localytics, gt_station_pv_uv_daily"""
    start_date = DateUtil.date2str(DateUtil.get_date_before_days(days * 219), '%Y-%m-%d')
    end_date = DateUtil.date2str(DateUtil.get_date_before_days(days*1), '%Y-%m-%d')
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
            `ios_weex_station_screen_main_open_uv`,
            `ios_weex_station_screen_main_open_pv`,
            `android_weex_station_screen_main_open_uv`,
            `android_weex_station_screen_main_open_pv`,
            `ios_weex_station_screen_info_uv`,
            `ios_weex_station_screen_info_pv`,
            `android_weex_station_screen_info_uv`,
            `android_weex_station_screen_info_pv`,
            `ios_weex_station_food_list_open_uv`,
            `ios_weex_station_food_list_open_pv`,
            `android_weex_station_food_list_open_uv`,
            `android_weex_station_food_list_open_pv`,
            `ios_weex_station_food_detail_open_uv`,
            `ios_weex_station_food_detail_open_pv`,
            `android_weex_station_food_detail_open_uv`,
            `android_weex_station_food_detail_open_pv`,
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
    hb_gt_travel_daily(1)
    # station_pv_uv(1)
