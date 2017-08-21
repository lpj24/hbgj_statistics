# -*- coding:utf-8 -*-
from dbClient.conf import localytics
import requests
import json


_api_key = localytics['api_key']
_api_secret = localytics['api_secret']
_api_root = localytics['api_root']
_app_id_android = localytics['app_id_android']
_app_id_ios = localytics['app_id_ios']
_metrics_pv_uv = ['sessions_per_event', 'users']


def request_pv(start_date, end_date, query_event, dimensions='day'):
    return _do_http(start_date, end_date, query_event, dimensions, 'sessions_per_event')


def request_uv(start_date, end_date, query_event, dimensions='day'):
    return _do_http(start_date, end_date, query_event, dimensions, 'users')


def _do_http(start_date, end_date, query_event, dimensions, metrics):
    a_id = _app_id_ios if query_event.count('ios') else _app_id_android
    data_params = {'app_id': a_id, 'dimensions': dimensions}
    conditions = {'event_name': query_event, 'day': ['between', start_date, end_date]}
    data_params["conditions"] = json.dumps(conditions)
    data_params['metrics'] = metrics
    try:
        return_data = requests.get(_api_root,
                                   auth=(_api_key, _api_secret), params=data_params, timeout=240)
    except (requests.RequestException,) as e:
        raise e
    if return_data.status_code != 200:
        raise return_data.raise_for_status()
    return_data = return_data.json()
    return return_data['results']
