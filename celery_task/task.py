# -*- coding: utf-8 -*-
from appCelery import job_celery


@job_celery.task
def add(a, b):
    return a + b


@job_celery.task
def day_job(days=1):
    print 'execute_day_job:' + str(days)