from celery import Celery

job_celery = Celery('celery_task', include=['celery_task.task'])
job_celery.config_from_object('celery_task.celeryconfig')
job_celery.send_task()

if __name__ == '__main__':
    job_celery.start()