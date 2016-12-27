# -*- coding: utf-8 -*-
from celery import Celery

app = Celery('hbgj_contab')

app.config_from_object('celery_task.celeryconfig')