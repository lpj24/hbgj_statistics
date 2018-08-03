# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
import re


# def get_phone_locale():
#     pattern = re.compile(r"[^\d]")
#     consumers_phone = """
#         select PHONE
#         from TICKET_ORDERDETAIL
#         left join TICKET_ORDER
#         on TICKET_ORDERDETAIL.ORDERID = TICKET_ORDER.ORDERID
#         left join phone_user
#         on phone_user.phoneid = TICKET_ORDER.PHONEID
#         where TICKET_ORDER.ORDERSTATUE not in (2,12,21,51,75)
#         and TICKET_ORDER.CREATETIME>='2016-01-01 00:00:00' AND TICKET_ORDER.CREATETIME<'2016-06-01 00:00:00'
#     """
#
#     locals_phone = """
#         select phone_num, city from phone_locale
#     """
#
#     insert_sql = """
#         insert into hbgj_consumers_phone (city, num) values (%s, %s)
#     """
#     query_data = DBCli().sourcedb_cli.query_all(consumers_phone)
#
#     phone_city = DBCli().targetdb_cli.query_all(locals_phone)
#     phone_locale = {}
#     phone_error = {"error_num": 0}
#     result_phone_city_count = {}
#     for x in phone_city:
#         # print x[1].encode('utf-8')
#         phone_locale[x[0]] = x[1]
#
#     for i in query_data:
#         phone = (i[0])[0:7]
#         if pattern.search(phone) is not None:
#             phone_error["error_num"] += 1
#         elif phone_locale.has_key(phone):
#             # city_key = (phone_locale[phone]).encode('utf-8')
#             city_key = phone_locale[phone]
#             if result_phone_city_count.has_key(city_key):
#                 result_phone_city_count[city_key] += 1
#             else:
#                 result_phone_city_count[city_key] = 0
#
#     for i in result_phone_city_count.items():
#         DBCli().targetdb_cli_test.insert(insert_sql, i)
#     print phone_error


def get_phone_locale_txt():
    pattern = re.compile(r"[^\d]")
    query_phone = """
        select phone from phone_user where phoneid=%s
    """
    locals_phone = """
        select phone_num, city from phone_locale
    """

    insert_sql = """
        insert into hbgj_consumers_phone (city, num) values (%s, %s)
    """

    phoneid_map_phone = """
        select phoneid, PHONE from phone_user
    """
    phoneid_map_phone_dict = {}
    phone_user_data = DBCli().sourcedb_cli.query_all(phoneid_map_phone)
    for x in phone_user_data:
        phoneid_map_phone_dict[str(x[0])] = (x[1]).strip()

    # phoneid_map_phone_dict['3029007'] = '15059515865'

    phone_city = DBCli().targetdb_cli.query_all(locals_phone)

    phone_error = {"error_num": 0}
    phone_sum = 0
    phone_locale = {}
    result_phone_city_count = {}

    for x in phone_city:
        # print x[1].encode('utf-8')
        phone_locale[x[0]] = x[1]
    with open("C:\\Users\\Administrator\\Desktop\\phoneid.txt", "r") as f:
        while 1:
            line = f.readline()
            line = line[0:-1]
            if line:
                phone_sum += 1
                phone = phoneid_map_phone_dict[str(line)]
                phone = phone[0:7]

                if pattern.search(phone) is not None:
                    phone_error["error_num"] += 1
                elif phone_locale.has_key(phone):
                    city_key = phone_locale[phone]
                    if result_phone_city_count.has_key(city_key):
                        result_phone_city_count[city_key] += 1
                    else:
                        result_phone_city_count[city_key] = 0

            else:
                break
    for i in result_phone_city_count.items():
        DBCli().targetdb_cli_test.insert(insert_sql, i)
    print phone_error, phone_sum

if __name__ == "__main__":
    # get_phone_locale()
    get_phone_locale_txt()