# -*- coding:utf-8 -*-
from datetime import timedelta
from celery.schedules import crontab


BROKER_URL = 'redis://127.0.0.1:6379/1'               # 指定 Broker
CELERY_RESULT_BACKEND = 'redis://127.0.0.1:6379/3'  # 指定 Backend

CELERY_TIMEZONE = 'Asia/Shanghai'

CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json', 'msgpack']

CELERYBEAT_SCHEDULE = {
    # 'add': {
    #     'task': 'celery_task.task.add',
    #     'schedule': timedelta(seconds=5),
    #     'args': [1, 4]
    # },

    # 'execute_job': {
    #     'task': 'time_job_excute.excute_day.execute_day_job',
    #     'schedule': crontab(hour=16, minute=24),
    #     'args': [1]
    # }
}
