# -*- coding: utf-8 -*-
import conf
import web
import requests
import signal


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


def handler_timeout():
    raise Exception


def time_out(fun):
    def wrapper():
        signal.signal(signal.SIGALRM, handler_timeout)
        signal.alarm(1*60*60*1.5)
        return fun()
    return wrapper


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
    # flight_info = get_airplane_info('3U8020', '2016-12-11', 'NGB', 'CTU')
    # #A320-214(SL)
    # print flight_info["data"]["aptype"]
    flight_info = get_airplane_info('HU7662', '2016-12-16')
    print flight_info["data"]
    # import datetime
    # from dbClient.dateutil import DateUtil
    #
    # start_date = datetime.date(2016, 12, 1)
    # end_date = datetime.date(2017, 2, 1)
    # while start_date <= end_date:
    #     flight_info = get_airplane_info('HU7662', DateUtil.date2str(start_date, '%Y-%m-%d'), 'HGH', 'CAN')
    #     # print flight_info
    #     print flight_info["data"]["aptype"]
    #     start_date = DateUtil().add_days(start_date, 1)