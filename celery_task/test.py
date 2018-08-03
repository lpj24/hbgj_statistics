if __name__ == '__main__':
    from celery_task.task import add
    r = add.delay(1, 3)
    print r.result