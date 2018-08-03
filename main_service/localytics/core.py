# -*- coding:utf-8 -*-
from dbClient.conf import localytics
import requests
import json
from functools import partial
# from collections import namedtuple

__all__ = ['request_pv', 'request_uv']
api_key = localytics['api_key']
api_secret = localytics['api_secret']
api_root = localytics['api_root']
app_id_android = localytics['app_id_android']
app_id_ios = localytics['app_id_ios']

app_id_gt_android = localytics['app_id_gt_android']
app_id_gt_ios = localytics['app_id_gt_ios']

metrics_pv_uv = ['sessions_per_event', 'users']


class LocalyticsRequest(object):

    @staticmethod
    def do_http(start_date, end_date, query_event, dimensions, metrics):
        a_id = app_id_ios if query_event.count('ios') else app_id_android
        data_params = {'app_id': a_id, 'dimensions': dimensions}
        conditions = {'event_name': query_event, 'day': ['between', start_date, end_date]}
        data_params["conditions"] = json.dumps(conditions)
        data_params['metrics'] = metrics
        try:
            return_data = requests.get(api_root,
                                       auth=(api_key, api_secret), params=data_params, timeout=240)
        except (requests.RequestException,) as e:
            raise e
        if return_data.status_code != 200:
            raise return_data.raise_for_status()
        return_data = return_data.json()['results']
        #
        # def map_error_key(key):
        #     if len(key.split(':')) > 1:
        #         return key.split(':')[1]
        #     return key
        # result = namedtuple('Result', map(map_error_key, return_data[0].keys()))

        # return [result(*(v.values())) for v in return_data]
        return return_data


request_pv = partial(LocalyticsRequest.do_http, metrics='sessions_per_event')
request_uv = partial(LocalyticsRequest.do_http, metrics='users')


if __name__ == '__main__':
    # print request_pv('2017-08-28', '2017-08-29', 'ios.weex.installment.pay.start', 'day')
    print request_pv('2018-07-12', '2018-07-12', 'ios.weex.installment.activated', 'day, a:type')
