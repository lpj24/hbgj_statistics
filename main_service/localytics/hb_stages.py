# -*- coding: utf-8 -*-
import requests
import json


def update_hbgj_stages_daily():
    api_key = "dd633143c1a14867726b60a-812924b6-5b0b-11e6-71ff-002dea3c3994"
    api_secret = "f91925eb1865c8431589ff2-81292808-5b0b-11e6-71ff-002dea3c3994"
    api_root = "https://api.localytics.com/v1/query"
    app_id_android = "2c64c068203c5033ddb127f-c76c5cc2-582a-11e5-07bf-00deb82fd81f"
    app_id_ios = "c0b8588071fc960755ee311-9ac01816-582a-11e5-ba3c-0013a62af900"
    event_list = ['ios.weex.installment.pay.start', ]

    data_params = {"app_id": app_id_ios, "dimensions": "day", "metrics": "sessions_per_event"}

    conditions = {"event_name": event_list[0], "day": ["between", "2017-08-06", "2017-08-07"]}

    data_params["conditions"] = json.dumps(conditions)

    r = requests.get(api_root, auth=(api_key, api_secret), params=data_params, timeout=240)
    result = r.json()
    print result["results"]

if __name__ == "__main__":
    update_hbgj_stages_daily()