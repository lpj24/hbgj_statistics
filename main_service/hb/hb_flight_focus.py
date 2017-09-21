# -*- coding: utf-8 -*-
from sql.hb_sqlHandlers import hb_flight_focus_user_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil
import ast
from collections import defaultdict
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


def update_flight_focus_user_daily(days=0):
    """更新航班关注用户, hbdt_focus_daily"""
    today = DateUtil.get_date_before_days(int(days))
    tomorrow = DateUtil.get_date_after_days(1-int(days))
    dto = [DateUtil.date2str(today, '%Y-%m-%d')] + \
          [DateUtil.date2str(tomorrow, '%Y-%m-%d'), DateUtil.date2str(today, '%Y-%m-%d')]

    query_data = DBCli().dynamic_focus_cli.queryOne(hb_flight_focus_user_sql['hb_flight_focus_users_daily'], dto)
    pv_sql = """
        SELECT count(*) FROM FLY_USERFOCUS_TBL
        where FOCUSTIME<%s
        and FOCUSTIME>=%s
    """
    pv_his_sql = """
        SELECT count(*) FROM FLY_USERFOCUS_TBL_HIS
        where FOCUSTIME<%s
        and FOCUSTIME>=%s
    """
    query_pv = DBCli().dynamic_focus_cli.queryOne(pv_sql, [DateUtil.date2str(tomorrow), DateUtil.date2str(today)])
    query_his_pv = DBCli().dynamic_focus_cli.queryOne(pv_his_sql, [DateUtil.date2str(tomorrow), DateUtil.date2str(today)])
    query_data = (query_data[0], query_data[1], int(query_pv[0]) + int(query_his_pv[0]))

    DBCli().targetdb_cli.insert(hb_flight_focus_user_sql['update_flight_focus_user_daily'], query_data)


def get_focus_new_user(days=0):
    start_date = DateUtil.get_date_before_days(int(days))
    end_date = DateUtil.get_date_after_days(1-int(days))
    hbdt_focus_file = DateUtil.date2str(start_date, "%Y-%m-%d") + "_hbdt_focus.dat"
    with open("/home/huolibi/data/hbdt/hbdt_focus/" + hbdt_focus_file) as hbdt_file:
        for hbdt_data in hbdt_file:
            try:
                (userid, phoneid, phone, token, flyid, focusdate, flydate
                 , createtime, platform, ordertype) = hbdt_data.strip().split("\t")
            except Exception:
                continue

            if ast.literal_eval(phoneid) is None:
                continue
            else:
                phone_id = str(userid) + '_' + str(phoneid)


def update_flight_focus_user_weekly():
    """更新航班关注用户周, hbdt_focus_weekly"""
    start_date, end_date = DateUtil.get_last_week_date()
    start_date = DateUtil.date2str(start_date, '%Y-%m-%d')
    end_date = DateUtil.date2str(end_date, '%Y-%m-%d')
    dto = {"s_day": start_date, "start_date": start_date, "end_date": end_date}
    query_data = DBCli().oracle_cli.queryOne(hb_flight_focus_user_sql['hb_flight_focus_users_weekly'], dto)
    DBCli().targetdb_cli.insert(hb_flight_focus_user_sql['update_flight_focus_user_weekly'], query_data)


def update_flight_focus_user_monthly():
    start_date, end_date = DateUtil.get_last_month_date()
    start_date = DateUtil.date2str(start_date, '%Y-%m-%d')
    end_date = DateUtil.date2str(end_date, '%Y-%m-%d')
    dto = {"s_day": start_date, "start_date": start_date, "end_date": end_date}
    query_data = DBCli().oracle_cli.queryOne(hb_flight_focus_user_sql['hb_flight_focus_users_monthly'], dto)
    DBCli().targetdb_cli.insert(hb_flight_focus_user_sql['update_flight_focus_user_monthly'], query_data)


def update_flight_focus_user_quarterly():
    start_date, end_date = DateUtil.get_last_quarter_date()
    start_date = DateUtil.date2str(start_date, '%Y-%m-%d')
    end_date = DateUtil.date2str(end_date, '%Y-%m-%d')
    dto = {"s_day": start_date, "start_date": start_date, "end_date": end_date}
    query_data = DBCli().oracle_cli.queryOne(hb_flight_focus_user_sql['hb_flight_focus_users_quarterly'], dto)
    DBCli().targetdb_cli.insert(hb_flight_focus_user_sql['update_flight_focus_user_quarterly'], query_data)


# def update_focus_platform(days):
#     uv_sql = """
#
        # select PLATFORM, uv from (
        # select PLATFORM, count(DISTINCT phoneid) uv from (
        #     SELECT phoneid, PLATFORM FROM FLY_USERFOCUS_TBL
        #     where PHONEID>0
        # and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
        # and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
        #
        #     union
        #     SELECT phoneid, PLATFORM FROM FLY_USERFOCUS_TBL_HIS
        #     where PHONEID>0
        # and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
        # and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
        # ) GROUP BY PLATFORM
        # UNION
        # select PLATFORM, count(DISTINCT userid) uv from (
        #     SELECT userid, PLATFORM from FLY_USERFOCUS_TBL
        #     where PHONEID=0
        # and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
        # and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
        #     UNION
        #     SELECT USERID, PLATFORM from FLY_USERFOCUS_TBL_HIS
        #     where PHONEID=0
        # and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
        # and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
        # ) GROUP BY PLATFORM ) where PLATFORM is not null
#
#     """
#
#     pv_sql = """
#         SELECT PLATFORM, count(*) PLATFORM FROM FLY_USERFOCUS_TBL
#         where CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
#         and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
#         and PLATFORM is not null GROUP BY PLATFORM
#     """
#     pv_his_sql = """
#         SELECT PLATFORM, count(*) FROM FLY_USERFOCUS_TBL_HIS
#         where CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
#         and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
#         and PLATFORM is not null GROUP BY PLATFORM
#     """
#
#     # today = DateUtil.get_date_before_days(int(days))
#     # tomorrow = DateUtil.get_date_after_days(1-int(days))
#     focus = open("focus_dat", 'a')
#     import datetime
#     start_date = datetime.date(2017, 1, 1)
#     end_date = datetime.date(2017, 1, 19)
#
#     while start_date < end_date:
#         platform_uv = {'android': 0, 'iphone': 0, 'jieji': 0, 'gtgj': 0, 'weixin': 0}
#         platform_pv = {'android': 0, 'iphone': 0, 'jieji': 0, 'gtgj': 0, 'weixin': 0}
#
#         tomorrow = DateUtil.add_days(start_date, 1)
#         dto = {"start_date": DateUtil.date2str(start_date, '%Y-%m-%d'),
#                "end_date":  DateUtil.date2str(tomorrow, '%Y-%m-%d')}
#         s_day = DateUtil.date2str(start_date, '%Y-%m-%d')
#         query_data = DBCli().oracle_cli.queryAll(uv_sql, dto)
#         for platform_data in query_data:
#             platform_type, uv = platform_data[0].lower(), platform_data[1]
#             if platform_type in platform_uv:
#                 platform_uv[platform_type] += int(uv)
#             elif platform_type.find("iphone") >= 0:
#                 platform_uv["iphone"] += 0
#
#         query_pv = DBCli().oracle_cli.queryAll(pv_sql, dto)
#         query_his_pv = DBCli().oracle_cli.queryAll(pv_his_sql, dto)
#
#         for k, v in dict(query_pv).items():
#             if k in platform_pv:
#                 platform_pv[k] += int(v)
#             elif k.find("iphone") >= 0:
#                 platform_pv["iphone"] += int(v)
#
#         for k, v in dict(query_his_pv).items():
#             if k in platform_pv:
#                 platform_pv[k] += int(v)
#             elif k.find("iphone") >= 0:
#                 platform_pv["iphone"] += int(v)
#
#         out_str = s_day + "\t" + str(platform_uv["android"]) + "\t" + str(platform_uv["iphone"]) + "\t" + \
#                   str(platform_uv["jieji"]) + "\t" + str(platform_uv["gtgj"]) + "\t" + str(platform_uv["weixin"]) \
#                   + "\t" + str(platform_pv["android"]) + "\t" + str(platform_pv["iphone"]) + "\t" + \
#                   str(platform_pv["jieji"]) + "\t" + str(platform_pv["gtgj"]) + "\t" + str(platform_pv["weixin"])
#
#         focus.write(out_str + "\n")
#
#         start_date = DateUtil.add_days(start_date, 1)


def update_platform_focus_by_file():
    from collections import defaultdict
    import ast
    focus_file = open("focus.dat", "a")
    every_day_data = defaultdict(dict)
    platform_uv = {'android': 0, 'iphone': 0, 'gtgj': 0, "jieji": 0, 'weixin': 0}

    with open("/home/huolibi/code/cal2017/cal0118_hbdt_focus/result/hbdt_focus.dat") as hbdt_focus_data:

        for hbdt_data in hbdt_focus_data:

            (userid, phoneid, flyid, focusdate, focusflydate, createtime, platform) = hbdt_data.strip().split("\t")
            if ast.literal_eval(phoneid) is None:
                if platform == "jieji":
                    phone_id = -1
                else:
                    continue
            else:
                phone_id = phoneid if int(phoneid) > 0 else userid

            create_time = createtime.split(" ")[0] if createtime else focusdate.split(" ")[0]

            if platform in platform_uv:
                platform = platform
            elif platform.lower().find("iphone") >= 0:
                platform = "iphone"

            try:
                (every_day_data[create_time])[platform].append(phone_id)
            except KeyError:
                (every_day_data[create_time])[platform] = [phone_id]

    with open("/home/huolibi/code/cal2017/cal0118_hbdt_focus/result/hbdt_focus_his.dat") as hbdt_focus_data:

        for hbdt_data in hbdt_focus_data:
            (userid, phoneid, flyid, focusdate, focusflydate, createtime, platform) = hbdt_data.strip().split("\t")
            if ast.literal_eval(phoneid) is None:
                if platform == "jieji":
                    phone_id = -1
                else:
                    continue
            else:
                phone_id = phoneid if int(phoneid) > 0 else userid
            create_time = createtime.split(" ")[0] if createtime else focusdate.split(" ")[0]

            if platform in platform_uv:
                platform = platform
            elif platform.lower().find("iphone") >= 0:
                platform = "iphone"

            try:
                (every_day_data[create_time])[platform].append(phone_id)
            except KeyError:
                (every_day_data[create_time])[platform] = [phone_id]

    for k, v in every_day_data.items():
        total_phone = []
        android_uv = iphone_uv = gtgj_uv = weixin_uv = total_uv = 0
        android_pv = iphone_pv = gtgj_pv = weixin_pv = jieji_pv = total_pv = 0
        for platform_k, platform_v in v.items():
            total_phone.extend(platform_v)
            if platform_k == "android":
                android_uv = len(set(platform_v))
                android_pv = len(platform_v)
            if platform_k == "iphone":
                iphone_uv = len(set(platform_v))
                iphone_pv = len(platform_v)
            if platform_k == "gtgj":
                gtgj_uv = len(set(platform_v))
                gtgj_pv = len(platform_v)
            if platform_k == "weixin":
                weixin_uv = len(set(platform_v))
                weixin_pv = len(platform_v)
            if platform_k == "jieji":
                jieji_pv = len(platform_v)
        total_uv = len(set(total_phone))
        total_pv = len(total_phone)
        out_str = k + "\t" + str(android_uv) + "\t" + str(iphone_uv) + "\t" + str(weixin_uv) + "\t" \
                  + str(gtgj_uv) + "\t" + str(0) + "\t" + str(total_uv) + "\t" + str(android_pv) + "\t" + \
                  str(iphone_pv) + "\t" + str(weixin_pv) + "\t" + str(gtgj_pv) + "\t" + str(jieji_pv) + "\t" + str(total_pv)
        focus_file.write(out_str + "\n")

    focus_file.close()


def update_focus_platform(days=0):
        app_sql = """

                select count(DISTINCT userid), platform from (
                select DISTINCT userid, platform  from fly_userfocus_tbl where createtime between to_date(:start_date, 'yyyy-mm-dd') and
                            to_date(:end_date, 'yyyy-mm-dd') and ordertype = 0
                            and (platform = 'android' or platform = 'iphone' or platform = 'iphonepro')
                            union
                            select DISTINCT userid, platform from FLY_USERFOCUS_TBL_HIS where createtime
                            between to_date(:start_date, 'yyyy-mm-dd') and
                            to_date(:end_date, 'yyyy-mm-dd') and ordertype = 0
                            and (platform = 'android' or platform = 'iphone' or platform = 'iphonepro')
                ) GROUP BY platform
        """

        weixin_sql = """
        select count(distinct userid) from (
            select distinct userid from fly_userfocus_tbl where createtime
            between to_date(:start_date, 'yyyy-mm-dd') and
            to_date(:end_date, 'yyyy-mm-dd') and ordertype = 0 and platform = 'weixin'
            union
            select distinct userid from FLY_USERFOCUS_TBL_HIS where createtime
            between to_date(:start_date, 'yyyy-mm-dd') and
            to_date(:end_date, 'yyyy-mm-dd') and ordertype = 0 and platform = 'weixin')
        """

        gtgj_sql = """
        select count(distinct userid) from (
            select distinct userid from fly_userfocus_tbl where createtime
            between to_date(:start_date, 'yyyy-mm-dd') and to_date(:end_date, 'yyyy-mm-dd') and
            (userid like 'gt%' or (ordertype = 0 and platform = 'gtgj'))
            union
            select distinct userid from FLY_USERFOCUS_TBL_HIS where createtime
            between to_date(:start_date, 'yyyy-mm-dd') and to_date(:end_date, 'yyyy-mm-dd') and
            (userid like 'gt%' or (ordertype = 0 and platform = 'gtgj'))

        )
        """

        jieji_sql = """
        select count(distinct token) from (
            select distinct token from fly_userfocus_tbl where createtime
            between to_date(:start_date, 'yyyy-mm-dd') and to_date(:end_date, 'yyyy-mm-dd')
            and ordertype = 0 and platform = 'jieji'
            union
            select distinct token from FLY_USERFOCUS_TBL_HIS where createtime
            between to_date(:start_date, 'yyyy-mm-dd') and to_date(:end_date, 'yyyy-mm-dd')
            and ordertype = 0 and platform = 'jieji'
        )
        """

        duanxin_sql = """
        select count(distinct phone) from (
            select distinct phone from fly_userfocus_tbl
            where createtime between to_date(:start_date, 'yyyy-mm-dd')
            and to_date(:end_date, 'yyyy-mm-dd') and ordertype = 1
            union
            select distinct phone from FLY_USERFOCUS_TBL_HIS
            where createtime between to_date(:start_date, 'yyyy-mm-dd')
            and to_date(:end_date, 'yyyy-mm-dd') and ordertype = 1)
        """

        total_sql = """
        select sum(count) from (
            select count(*) as count from fly_userfocus_tbl where createtime
            between to_date(:start_date, 'yyyy-mm-dd')
            and to_date(:end_date, 'yyyy-mm-dd')
            union
            select count(*) as count from FLY_USERFOCUS_TBL_HIS where createtime
            between to_date(:start_date, 'yyyy-mm-dd')
            and to_date(:end_date, 'yyyy-mm-dd')
            )
        """

        app_sql_pv = """

                select count(userid), platform from (
                select userid, platform  from fly_userfocus_tbl where createtime between to_date(:start_date, 'yyyy-mm-dd') and
                            to_date(:end_date, 'yyyy-mm-dd') and ordertype = 0
                            and (platform = 'android' or platform = 'iphone' or platform = 'iphonepro')
                            union all
                    select userid, platform from FLY_USERFOCUS_TBL_HIS where createtime
                    between to_date(:start_date, 'yyyy-mm-dd') and
                    to_date(:end_date, 'yyyy-mm-dd') and ordertype = 0
                    and (platform = 'android' or platform = 'iphone' or platform = 'iphonepro')
                ) GROUP BY platform
        """

        weixin_sql_pv = """
        select sum(pv) from (
            select count(*) pv from fly_userfocus_tbl where createtime
            between to_date(:start_date, 'yyyy-mm-dd') and
            to_date(:end_date, 'yyyy-mm-dd') and ordertype = 0 and platform = 'weixin'
            union
            select count(*) pv from FLY_USERFOCUS_TBL_HIS where createtime
            between to_date(:start_date, 'yyyy-mm-dd') and
            to_date(:end_date, 'yyyy-mm-dd') and ordertype = 0 and platform = 'weixin')
        """

        gtgj_sql_pv = """
        select sum(pv) from (
            select count(*) pv from fly_userfocus_tbl where createtime
            between to_date(:start_date, 'yyyy-mm-dd') and to_date(:end_date, 'yyyy-mm-dd') and
            (userid like 'gt%' or (ordertype = 0 and platform = 'gtgj'))
            union
            select count(*) pv from FLY_USERFOCUS_TBL_HIS where createtime
            between to_date(:start_date, 'yyyy-mm-dd') and to_date(:end_date, 'yyyy-mm-dd') and
            (userid like 'gt%' or (ordertype = 0 and platform = 'gtgj'))
        )
        """

        jieji_sql_pv = """
        select sum(pv) from (
            select count(*) pv from fly_userfocus_tbl where createtime
            between to_date(:start_date, 'yyyy-mm-dd') and to_date(:end_date, 'yyyy-mm-dd')
            and ordertype = 0 and platform = 'jieji'
            union
            select count(*) pv from FLY_USERFOCUS_TBL_HIS where createtime
            between to_date(:start_date, 'yyyy-mm-dd') and to_date(:end_date, 'yyyy-mm-dd')
            and ordertype = 0 and platform = 'jieji'
        )
        """

        duanxin_sql_pv = """
        select sum(pv) from (
            select count(*) pv from fly_userfocus_tbl
            where createtime between to_date(:start_date, 'yyyy-mm-dd')
            and to_date(:end_date, 'yyyy-mm-dd') and ordertype = 1
            union
            select count(*) pv from FLY_USERFOCUS_TBL_HIS
            where createtime between to_date(:start_date, 'yyyy-mm-dd')
            and to_date(:end_date, 'yyyy-mm-dd') and ordertype = 1)
        """

        import datetime
        start_date = datetime.date(2015, 4, 14)
        end_date = datetime.date(2017, 1, 1)
        focus = open("new_focus_jieji_duanxin.dat", "a")
        while start_date < end_date:
            next_day = DateUtil.date2str(DateUtil.add_days(start_date, 1), '%Y-%m-%d')
            dto = {"start_date": DateUtil.date2str(start_date, '%Y-%m-%d'), "end_date": next_day}
            # app_data = DBCli().oracle_cli.queryAll(app_sql, dto)
            # iphone_num = 0
            # android_num = 0
            # for app in app_data:
            #     app_num, platform = app[0], app[1]
            #     if platform in ["iphone", "iphonepro"]:
            #         iphone_num += app_num
            #     else:
            #         android_num = app_num
            # weixin_data = DBCli().oracle_cli.queryOne(weixin_sql, dto)
            # gtgj_data = DBCli().oracle_cli.queryOne(gtgj_sql, dto)
            jieji_data = DBCli().oracle_cli.queryOne(jieji_sql, dto)
            # total_data = DBCli().oracle_cli.queryOne(total_sql, dto)
            duanxin_data = DBCli().oracle_cli.queryOne(duanxin_sql, dto)

            # app_data_pv = DBCli().oracle_cli.queryAll(app_sql_pv, dto)
            # iphone_num_pv = 0
            # android_num_pv = 0
            # for app in app_data_pv:
            #     app_num, platform = app[0], app[1]
            #     if platform in ["iphone", "iphonepro"]:
            #         iphone_num_pv += app_num
            #     else:
            #         android_num_pv = app_num
            # weixin_data_pv = DBCli().oracle_cli.queryOne(weixin_sql_pv, dto)
            # gtgj_data_pv = DBCli().oracle_cli.queryOne(gtgj_sql_pv, dto)
            jieji_data_pv = DBCli().oracle_cli.queryOne(jieji_sql_pv, dto)
            duanxin_data_pv = DBCli().oracle_cli.queryOne(duanxin_sql_pv, dto)

            # out_str = DateUtil.date2str(start_date, '%Y-%m-%d') + "\t" + str(android_num) + \
            #           "\t" + str(iphone_num)+ "\t" + str(weixin_data[0]) + "\t" + \
            #           str(gtgj_data[0]) + "\t" + str(jieji_data[0]) + "\t" + str(duanxin_data[0]) \
            #           + "\t" + str(android_num_pv) + \
            #           "\t" + str(iphone_num_pv)+ "\t" + str(weixin_data_pv[0]) + "\t" + \
            #           str(gtgj_data_pv[0]) + "\t" + str(jieji_data_pv[0]) + "\t" + str(duanxin_data_pv[0]) + \
            #           "\t" + str(total_data[0])
            out_str = DateUtil.date2str(start_date, '%Y-%m-%d') + "\t" + str(jieji_data[0]) + "\t" + str(duanxin_data[0]) + \
                      "\t" + str(jieji_data_pv[0]) + "\t" + str(duanxin_data_pv[0])
            focus.write(out_str + "\n")

            start_date = DateUtil.add_days(start_date, 1)
        focus.close()


def update_hb_focus_inter_inland(days=0):
    """更新国内外航班关注pv及uv, hbdt_focus_daily_inter"""
    start_date = DateUtil.get_date_before_days(int(days))
    end_date = DateUtil.get_date_after_days(1-int(days))
    inter_inland_sql = """
        SELECT USERID,FLYKEY
        FROM FLY_USERFOCUS_TBL WHERE FOCUSTIME>=%s and FOCUSTIME <%s
        union all
        SELECT USERID,FLYKEY
        FROM FLY_USERFOCUS_TBL_HIS WHERE FOCUSTIME>=%s and FOCUSTIME <%s
    """

    inter_sql = """
        select THREE_WORDS_CODE from AIRPORT_INTER_INFO
    """

    insert_sql = """
        insert into hbdt_focus_daily_inter
        (s_day, pv_inland, pv_inter, uv_inland, uv_inter, uv, createtime, updatetime)
        values
        (%s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now() ,
        s_day = VALUES(s_day),
        pv_inland = VALUES(pv_inland),
        pv_inter = VALUES(pv_inter),
        uv_inland = VALUES(uv_inland),
        uv_inter = VALUES(uv_inter),
        uv = VALUES(uv)
    """

    inter_codes = DBCli().oracle_cli.queryAll(inter_sql)
    inter_codes = [code[0] for code in inter_codes]
    dto = [start_date, end_date, start_date, end_date]
    inter_inland_data = DBCli().dynamic_focus_cli.queryAll(inter_inland_sql, dto)

    fly_uv = defaultdict(list)
    inter_uv_num = 0
    inland_uv_num = 0
    inter_pv_num = 0
    inland_pv_num = 0

    for data in inter_inland_data:
        user_id, fly_key = data
        fly_info = fly_key.split(",")
        fly_uv[user_id].append([fly_info[2], fly_info[3]])

    for k, v in fly_uv.items():
        tmp_inter_uv = 0
        tmp_inland_uv = 0
        for fly in v:
            dep, arr = fly
            if dep in inter_codes or arr in inter_codes:
                tmp_inter_uv += 1
            else:
                tmp_inland_uv += 1

        if tmp_inter_uv > 0:
            # 有一个航班是国际, inter_uv = 1  inland_uv=0, pv分开算
            # 如果2个国际, 2个国内, inter_pv = 2, inland_pv = 2
            inter_uv_num += 1
            inland_uv_num += 0
            inter_pv_num += tmp_inter_uv
            inland_pv_num += tmp_inland_uv
        else:
            inland_uv_num += 1
            inter_uv_num += 0
            inland_pv_num += tmp_inland_uv
            inter_pv_num += 0

    DBCli().targetdb_cli.insert(insert_sql, [DateUtil.date2str(start_date, '%Y-%m-%d'),
                                             inland_pv_num, inter_pv_num, inland_uv_num, inter_uv_num, len(fly_uv)])
    pass


def tmp_cal_inter_inland(codes_city):
    import datetime
    start_date = datetime.date(2016, 12, 15)
    end_date = datetime.date(2017, 3, 16)
    dto = [start_date, end_date, start_date, end_date]
    sql = """
        SELECT USERID,FLYKEY
        FROM FLY_USERFOCUS_TBL WHERE FOCUSTIME>=%s and FOCUSTIME <%s
        union all
        SELECT USERID,FLYKEY
        FROM FLY_USERFOCUS_TBL_HIS WHERE FOCUSTIME>=%s and FOCUSTIME <%s
    """
    inter_sql = """
        select THREE_WORDS_CODE from AIRPORT_INTER_INFO
    """

    inland_sql = """
        select THREE_WORDS_CODE from AIRPORT_NATION_INFO
    """
    inter_codes = DBCli().oracle_cli.queryAll(inter_sql)
    inter_codes = [code[0] for code in inter_codes]

    inland_codes = DBCli().oracle_cli.queryAll(inland_sql)
    inland_codes = [code[0] for code in inland_codes]

    # NAY与PEK 是北京机场
    # SHA与PVG 是上海机场

    fly_info = DBCli().dynamic_focus_cli.queryAll(sql, dto)
    fly_line = defaultdict(list)
    fly_line_inland = defaultdict(list)
    fly_line_result = defaultdict(int)
    fly_line_inland_result = defaultdict(int)

    inter_file = open("inter_file.dat", "a")
    inland_file = open("inland_file.dat", "a")
    for data in fly_info:
        user_id, fly_key = data
        fly_info = fly_key.split(",")
        dep, arr = fly_info[2], fly_info[3]
        # if dep == "NAY":
        #     dep = "PEK"
        # elif dep == "PVG":
        #     dep = "SHA"
        # elif arr == "PVG":
        #     arr = "SHA"
        # elif arr == "NAY":
        #     arr = "PEK"

        if dep in inter_codes or arr in inter_codes:
            # 国际航班
            try:
                if dep in inland_codes:
                    # 关注国际航班的  起飞地是国内
                    dep_inland = codes_city[dep]
                    arr_inland = codes_city[arr]
                    fly_line_inland[dep_inland + "-" + arr_inland].append(user_id)
                dep = codes_city[dep]
                arr = codes_city[arr]
                fly_line[dep + "-" + arr].append(user_id)
            except (KeyError, ):
                continue

    for k, v in fly_line.items():
        fly_line_result[k] = len(v)

    for k, v in fly_line_inland.items():
        fly_line_inland_result[k] = len(v)

    i = 0
    for k, v in sorted(fly_line_result.iteritems(), key=lambda a: a[1], reverse=True):
        if i > 50:
            break
        inter_file.write("\t".join(k.split("-")) + "\t" + str(v) + "\n")
        i += 1

    i = 0
    print "---------------------------------------------------------"
    for k, v in sorted(fly_line_inland_result.iteritems(), key=lambda a: a[1], reverse=True):
        if i > 20:
            break
        # print "\t".join(k.split("-")) + "\t" + str(v)
        inland_file.write("\t".join(k.split("-")) + "\t" + str(v) + "\n")
        i += 1


if __name__ == "__main__":
    update_flight_focus_user_daily(1)
