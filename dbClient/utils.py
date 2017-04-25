# -*- coding: utf-8 -*-
import conf
import web
import requests
import signal
import time, logging
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


def execute_day_job_again(table_name, fun_path, fun_name, execute_day=1):
    import os
    fun_path = (fun_path.split("\\"))[-3:]
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
    delete_sql = "delete from " + table_name + " where s_day=%s"
    DBCli().targetdb_cli.batchInsert(delete_sql, [s_day])
    os.system("python ./tmp_py.py")
    os.remove("./tmp_py.py")


def storage_execute_job(f_path, f_name, f_doc):
    from dbClient.db_client import DBCli
    renewable = 1 if f_path else 0
    job_type = 1
    if f_name == "hbgj_user":
        job_type = 5
    f_doc = f_doc.split(",")
    f_des, f_table = f_doc
    insert_sql = """
        insert into bi_execute_job (job_name, job_path, job_doc, job_table, job_type, renewable, createtime, updatetime)
        values (%s, %s, %s, %s, %s, %s, now(), now())
    """
    DBCli().targetdb_cli.insert(insert_sql, [f_name, f_path, f_des, f_table, job_type, renewable])


def handler_timeout():
    raise Exception


def time_out(fun):
    def wrapper():
        signal.signal(signal.SIGALRM, handler_timeout)
        signal.alarm(1*60*60*1.5)
        return fun()
    return wrapper


def exeTime(func):
    def newFunc(*args, **kwargs):
        start = time.time()
        back = func(*args, **kwargs)
        logging.warning("run time " + str(time.time() - start) + "s")
        return back
    return newFunc


@exeTime
def get_airplane_info(flightno, date, depcode, arrcode):
    url = "http://58.83.130.92:7070/pysrv/flightservice/airplane_by_flightno/"

    params = {
        "flightno": flightno,
        "date": date,
        "depcode": depcode,
        "arrcode": arrcode
    }
    result = requests.post(url, data=params)

    return result.json()

if __name__ == "__main__":
    import sys
    t_name = sys.argv[1]
    f_path = sys.argv[2]
    f_name = sys.argv[3]
    e_day = sys.argv[4]
    execute_day_job_again(t_name, f_path, f_name, e_day)