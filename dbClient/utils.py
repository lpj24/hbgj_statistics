# -*- coding: utf-8 -*-
import conf
import web
import threading, logging
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def getMailServer():
    web.config.smtp_server = conf.mail["mail_server"]
    web.config.smtp_port = conf.mail["port"]
    web.config.smtp_username = conf.mail["username"]
    web.config.smtp_password = conf.mail["password"]
    web.config.smtp_starttls = True
    mailSer = web
    return mailSer


def sendMail(mail, msgText, subject):
    mailSer = getMailServer()
    send_address = "<"+mailSer.config.smtp_username+">"
    mailSer.sendmail(send_address, mail, subject, msgText,
                     headers=({'Content-Type': 'text/html;charset=utf-8', 'User-Agent': 'webpy.sendmail',
                               'X-Mailer': 'webpy.sendmail'}))


def execute_day_job_again(table_name, fun_path, fun_name, job_type, execute_day=1):
    import os
    fun_path = (fun_path.split(os.path.sep))[-3:]
    fun_path[-1] = (fun_path[-1].split("."))[0]
    fun_path = ".".join(fun_path)
    with open("tmp_py.py", "w") as py_file:
        coding_str = "# -*- coding: utf-8 -*-\n"
        import_str = "from " + fun_path + " import " + fun_name + "\n"
        main_str = "if __name__ == '__main__':\n"
        execute_fun_str = "\t" + fun_name + "(" + str(execute_day) + ")" + "\n"
        py_str = coding_str + import_str + main_str + execute_fun_str
        py_file.write(py_str)
    # del execute day data
    s_day = DateUtil.date2str(DateUtil.get_date_before_days(int(execute_day)), "%Y-%m-%d")
    if job_type != 5:
        for table in table_name:
            delete_sql = "delete from " + table + " where s_day=%s"
            DBCli().targetdb_cli.batchInsert(delete_sql, [s_day])
    os.system("python ./tmp_py.py")
    os.remove("tmp_py.py")


def storage_execute_job(f_path, f_name, f_doc):
    renewable = 1 if f_path else 0
    job_type = 5 if f_name == "hbgj_user" else 1
    f_des, f_table = f_doc.split(",")
    insert_sql = """
        insert into bi_execute_job (job_name, job_path, job_doc, job_table, job_type, renewable, createtime, updatetime)
        values (%s, %s, %s, %s, %s, %s, now(), now())
    """
    DBCli().targetdb_cli.insert(insert_sql, [f_name, f_path, f_des.strip(), f_table.strip(), job_type, renewable])


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
                fun_path = fun(int(self.days))
                fun_name = fun.__name__
                fun_doc = fun.__doc__
                check_fun = DBCli().redis_cli.sismember("execute_day_job", fun_name)
                if not check_fun:
                    if fun_path and fun_path.endswith("pyc"):
                        fun_path = fun_path[0: -1]
                    storage_execute_job(fun_path, fun_name, fun_doc)
                    DBCli().redis_cli.sadd("execute_day_job", fun_name)
                self.queue.task_done()
            except Exception as e:
                logging.warning(str(fun) + "---" + str(e.message) + "---" + str(e.args))
                self.queue.task_done()
                continue


def execute_job_thread_pool(queue, arg):
    """
    :param queue: 队列
    :param arg:   函数的参数
    :return:
    """
    for i in xrange(6):
        t = ThreadExecuteJob(queue, arg)
        t.setName("thread" + str(i))
        t.setDaemon(True)
        t.start()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(usage='python -table hbgj gtgj gt_consumers -path', description='this is a test')
    parser.add_argument('-table', required=True, type=str, nargs="*",
                        help='gt_user hb_consumers, 要操作的目标数据表, 多张表使用空格分离')
    parser.add_argument('-path', required=True, type=str, help='操作py文件的具体路径')
    parser.add_argument('-name', required=True, type=str, help='操作的函数名')
    parser.add_argument('-day', required=True, type=str, help='更新的天数')
    parser.add_argument('-jobType', required=True, type=int, help='更新的任务类型')
    args = parser.parse_args()
    t_name = args.table
    f_path = args.path
    f_name = args.name
    e_day = args.day
    j_type = args.jobType
    execute_day_job_again(t_name, f_path, f_name, j_type, e_day)