# -*- coding:utf-8 -*-

BROKER_URL = 'redis://127.0.0.1:6379'               # 指定 Broker
CELERY_RESULT_BACKEND = 'redis://127.0.0.1:6379/3'  # 指定 Backend

CELERY_TIMEZONE = 'Asia/Shanghai'

CELERY_IMPORTS = (                                  # 指定导入的任务模块
    'time_job_excute.excute_day'
)