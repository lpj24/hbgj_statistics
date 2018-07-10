# -*- coding: utf-8 -*-
import requests
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import json
import datetime
import core
import time


def update_hb_gt_sum(days=0):
    app_hbgj_android_id = core.app_id_android
    app_hbgj_ios_id = core.app_id_ios

    app_gtgj_android_id = core.app_id_gt_android
    app_gtgj_ios_id = core.app_id_gt_ios

    search_app = [app_hbgj_android_id, app_hbgj_ios_id, app_gtgj_android_id, app_gtgj_ios_id]
    search_app_name = ["hbgj_android", "hbgj_ios", "gtgj_android", "gtgj_ios"]
    req_url = "https://api.localytics.com/v1/query"
    api_key = core.api_key
    api_secret = core.api_secret

    search_date = datetime.date(2015, 11, 1)
    start_date, end_date = DateUtil.get_last_month_date(search_date)

    while 1:
        end_date = end_date - datetime.timedelta(1)
        for app in zip(search_app, search_app_name):
            app_id, app_name = app
            data_params = {
                "app_id": app_id,
                "conditions": json.dumps({"day": ["between", str(start_date), str(end_date)]})
            }
            for m in ["average_session_length", "users, sessions"]:
                data_params["metrics"] = m
                response_data = requests.get(req_url, auth=(api_key, api_secret), params=data_params)
                response_users_sessions = response_data.json()["results"][0]
                if m == "average_session_length":
                    average_session_length = response_users_sessions["average_session_length"]
                    # if average_session_length > 60:
                    #     average_session_length = str(average_session_length/60) + "m" + " " + str(average_session_length - (average_session_length/60)*60) + "s"
                    # else:
                    average_session_length = str(average_session_length) + "s"
                else:
                    print response_users_sessions
                    sessions = response_users_sessions['sessions']
                    users = response_users_sessions['users']
                    bit = round(float(sessions)/float(users), 3)
            print str(start_date) + "\t" + app_name + "\t" + str(sessions) + "\t" + average_session_length + "\t" + str(users) + "\t" + str(bit)
        start_date, end_date = DateUtil.get_last_month_date(start_date)


if __name__ == '__main__':

    update_hb_gt_sum(1)