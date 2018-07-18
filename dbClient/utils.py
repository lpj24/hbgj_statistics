# -*- coding: utf-8 -*-
import conf
import web
import threading, logging
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import inspect
import subprocess
import os


def get_mail_server():
    web.config.smtp_server = conf.mail["mail_server"]
    web.config.smtp_port = conf.mail["port"]
    web.config.smtp_username = conf.mail["username"]
    web.config.smtp_password = conf.mail["password"]
    web.config.smtp_starttls = True
    mail_server = web
    return mail_server


def sendMail(mail, msgText, subject):
    mailSer = get_mail_server()
    send_address = "<"+mailSer.config.smtp_username+">"
    mailSer.sendmail(send_address, mail, subject, msgText,
                     headers=({'Content-Type': 'text/html;charset=utf-8', 'User-Agent': 'webpy.sendmail',
                               'X-Mailer': 'webpy.sendmail'}))


def execute_day_job_again(table_name, fun_path, fun_name, job_type, execute_day=1):
    fun_path = (fun_path.split(os.path.sep))[-3:]
    fun_path[-1] = (fun_path[-1].split("."))[0]
    fun_path = ".".join(fun_path)
    import_str = "from " + fun_path + " import " + fun_name + ";"
    execute_fun_str = import_str + fun_name + "(" + str(execute_day) + ")" + ";"
    py_str = execute_fun_str

    s_day = DateUtil.date2str(DateUtil.get_date_before_days(int(execute_day)), "%Y-%m-%d")
    if job_type != 5:
        for table in table_name:
            delete_sql = "delete from " + table + " where s_day=%s"
            DBCli().targetdb_cli.batch_insert(delete_sql, [s_day])
    os.system("python -c " + '"' + py_str + '"')


def storage_execute_job(fun):
    category_type = {
        'hb': 1,
        'gt': 2,
        'huoli': 3,
        'localytics': 4
    }
    fun_name = fun.__name__
    fun_doc = inspect.getdoc(fun)
    fun_path = inspect.getfile(fun)
    fun_category = fun_path.split(os.path.sep)
    category_job = fun_category[-2]
    category_job_id = category_type.get(category_job, 5)
    if fun_doc is None:
        return
    check_fun = DBCli().redis_cli.sismember("execute_day_job", fun_name)
    if not check_fun:
        if fun_path and fun_path.endswith("pyc"):
            fun_path = fun_path[0: -1]
        is_execute = 1 if fun_path else 0
        job_type = 5 if (fun_name == "hbgj_user" or fun_name == "send_hb_coupon_delay_eamil_daily") else 1
        f_des, f_table = fun_doc.split(",")
        insert_sql = """
            insert into bi_execute_job (job_name, job_path, job_doc, job_table, job_type, is_execute, category_type_id, 
            create_time, update_time)
            values (%s, %s, %s, %s, %s, %s, %s, now(), now())
            on duplicate key update update_time = now(),
            job_name = values(job_name),
            job_path = values(job_path),
            job_doc = values(job_doc),
            job_table = values(job_table),
            job_type = values(job_type),
            is_execute = values(is_execute),
            category_type_id = values(category_type_id)
        """
        try:
            DBCli().targetdb_cli.insert(insert_sql,
                                    [fun_name, fun_path, f_des.strip(), f_table.strip(), job_type, is_execute,
                                     category_job_id])
        except Exception as e:
            logging.error(e.message + fun_name)
        else:
            DBCli().redis_cli.sadd("execute_day_job", fun_name)


class ThreadExecuteJob(threading.Thread):
    """Threaded Url Grab"""

    def __init__(self, queue, days):
        threading.Thread.__init__(self)
        self.queue = queue
        self.days = days

    def run(self):
        while 1:
            if self.queue.empty():
                break
            fun = self.queue.get()
            try:
                fun(int(self.days))

            except Exception as e:
                logging.error(str(fun) + "---" + str(e.message) + "---" + str(e.args))
                continue
            finally:
                storage_execute_job(fun)
                self.queue.task_done()


def execute_job_thread_pool(queue, arg):
    """
    :param queue: 队列
    :param arg:   函数的参数
    :return:
    """
    for i in xrange(6):
        t = ThreadExecuteJob(queue, arg)
        t.setName("thread" + str(i))
        t.start()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(usage='python -table hbgj gtgj gt_consumers -path', description='this is a test')
    parser.add_argument('-table', required=True, type=str, nargs="*",
                        help='gt_user hb_consumers, 要操作的目标数据表, 多张表使用空格分离')
    parser.add_argument('-path', required=True, type=str, help='操作py文件的具体路径')
    parser.add_argument('-name', required=True, type=str, help='操作的函数名')
    parser.add_argument('-day', required=True, type=str, help='的天数')
    parser.add_argument('-jobType', required=True, type=int, help='的任务类型')
    args = parser.parse_args()
    t_name = args.table
    f_path = args.path
    f_name = args.name
    e_day = args.day
    j_type = args.jobType
    execute_day_job_again(t_name, f_path, f_name, j_type, e_day)