# -*- coding: utf-8 -*-
import pandas
from collections import Iterable


def flatten(items):
    for item in items:
        if isinstance(item, (tuple, list)):
            for subitem in flatten(item):
                yield subitem
        else:
            yield item

if __name__ == "__main__":
    # pd_file = pandas.read_excel("C:\\Users\\Administrator\\Desktop\\16_15.xlsx")
    # print pd_file
    s = [1, 2, 3, [8, 9, [-1, -2, -3], 22], 9, 10, 11, [12, 14, 16]]

# import time
# from celery import Celery
#
#
# broker = 'redis://127.0.0.1:6379'
# backend = 'redis://127.0.0.1:6379/0'
# app = Celery('my_task', broker=broker, backend=backend)
#
# @app.task
# def add(x, y):
#     time.sleep(5)
#     return x + y