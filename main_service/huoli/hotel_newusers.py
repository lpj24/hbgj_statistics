import re
import os
import logging
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import tarfile


def update_hotel_newusers_daily(days=0):
    regex = re.compile(r"uid=([0-9]*)")
    s_day = DateUtil.date2str(DateUtil.getDateBeforeDays(days), "%Y%m%d")
    file_list = [os.path.join("/home/huolibi/external_data/hotel_log", "access.log_207_" + s_day + ".tar.gz"),
                 os.path.join("/home/huolibi/external_data/hotel_log", "access.log_209_" + s_day + ".tar.gz")]
    # file_list = [os.path.join("C:\\Users\\Administrator\\Desktop", "access.log_207_" + s_day + ".tar.gz"),
    #              os.path.join("C:\\Users\\Administrator\\Desktop", "access.log_209_" + s_day + ".tar.gz")]
    # regex = re.compile(regex)
    #uid=(([0-9]|[a-z])*)
    if os.path.isfile(file_list[0]) and os.path.isfile(file_list[1]):
        uid_key = s_day + "_log_uid"
        for tar_file in file_list:
            tar = tarfile.open(tar_file)
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

        dto = [DateUtil.date2str(DateUtil.getDateBeforeDays(days), "%Y-%m-%d"), today_uid_num]
        DBCli().targetdb_cli.insert(insert_sql, dto)

    else:
        logging.warn("no file" + s_day + "log")

if __name__ == "__main__":
    update_hotel_newusers_daily(2)