# -*- coding: utf-8 -*-
import re
import os
import logging
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import tarfile


def update_hotel_newusers_daily(days=0):
    """更新酒店新用户, hotel_newusers_daily"""
    if not check_hotel_newusers(days):
        return
    regex = re.compile(r"uid=([0-9]*)")
    s_day = DateUtil.date2str(DateUtil.get_date_before_days(days), "%Y%m%d")
    file_list = [os.path.join("/home/huolibi/external_data/hotel_log", "access.log_207_" + s_day + ".tar.gz"),
                 os.path.join("/home/huolibi/external_data/hotel_log", "access.log_209_" + s_day + ".tar.gz")]
    # file_list = [os.path.join("C:\\Users\\Administrator\\Desktop", "access.log_207_" + s_day + ".tar.gz"),
    #              os.path.join("C:\\Users\\Administrator\\Desktop", "access.log_209_" + s_day + ".tar.gz")]
    # regex = re.compile(regex)
    # uid=(([0-9]|[a-z])*)
    if False in map(os.path.isfile, file_list):
        logging.warn("no file" + s_day + "log")
    else:
        uid_key = s_day + "_log_uid"
        for tar_file in file_list:
            try:
                tar = tarfile.open(tar_file)
            except Exception:
                logging.warning(str(tar_file) + " file error")
                return
            filename = (tar.getnames())[0]
            log_file = "access.log_" + s_day
            tar.extract(filename)
            tar.close()
            with open(log_file, "r") as f:
                while 1:
                    line = f.readline()
                    if line:
                        uid = regex.findall(line)
                        if len(uid) > 0:
                            DBCli().redis_cli.sadd(uid_key, uid)
                    else:
                        break
            os.remove(log_file)
        DBCli().redis_cli.sunionstore(s_day + "_activeusers", s_day + "_activeusers", uid_key)
        today_uid_num = DBCli().redis_cli.sdiffstore(uid_key, uid_key, "total_log_uids")
        DBCli().redis_cli.sunionstore("total_log_uids", "total_log_uids", uid_key)
        insert_sql = """
                insert into hotel_newusers_daily values (%s, %s, now(), now())
                on duplicate key update updatetime = now(),
                s_day = VALUES(s_day),
                new_users = VALUES(new_users)
            """

        dto = [DateUtil.date2str(DateUtil.get_date_before_days(days), "%Y-%m-%d"), today_uid_num]
        DBCli().targetdb_cli.insert(insert_sql, dto)
        DBCli().redis_cli.expire(uid_key, 86400)

def check_hotel_newusers(days):
    s_day = DateUtil.date2str(DateUtil.get_date_before_days(days + 1), "%Y-%m-%d")
    sql = """
        select count(*) from hotel_newusers_daily where s_day=%s 
    """
    last_count = DBCli().targetdb_cli.query_one(sql, [s_day])
    print last_count[0]
    if last_count[0] == 1:
        return True
    else:
        return False


if __name__ == "__main__":
    # update_hotel_newusers_daily(0)
    print check_hotel_newusers(-1)