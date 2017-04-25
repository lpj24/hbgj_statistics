# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_focus_platform(days=0):
    """更新不同平台关注数据(日), hbdt_focus_platform_daily"""
    all_platform_sql_uv = """
        select platform, count(distinct userid) from (
            select distinct(userid) userid, platform from fly_userfocus_tbl
            where createtime between to_date(:start_date, 'yyyy-mm-dd') and to_date(:end_date, 'yyyy-mm-dd')
            and ordertype = 0 and (platform = 'android' or platform = 'iphone'
            or platform = 'iphonepro' or platform = 'web' or platform='weixin')
            and userid not like 'gt%'
            union
            select distinct(userid) userid, platform from fly_userfocus_tbl_his
            where createtime between to_date(:start_date, 'yyyy-mm-dd') and to_date(:end_date, 'yyyy-mm-dd')
            and ordertype = 0 and (platform = 'android' or platform = 'iphone'
            or platform = 'iphonepro' or platform = 'web' or platform='weixin')
            and userid not like 'gt%') group by platform
    """
    pv_sql = """
        SELECT platform, count(*) FROM FLY_USERFOCUS_TBL
        where CREATETIME<to_date(:end_date, 'yyyy-mm-dd')
        and CREATETIME>=to_date(:start_date, 'yyyy-mm-dd')
        and ordertype = 0
        and platform in ('android', 'weixin', 'iphone', 'iphonepro', 'web')
        and userid not like 'gt%'
        group by platform
    """
    pv_his_sql = """
        SELECT platform, count(*) FROM FLY_USERFOCUS_TBL_HIS
        where CREATETIME<to_date(:end_date, 'yyyy-mm-dd')
        and CREATETIME>=to_date(:start_date, 'yyyy-mm-dd')
        and ordertype = 0
        and platform in ('android', 'weixin', 'iphone', 'iphonepro', 'web')
        and userid not like 'gt%'
        group by platform
    """

    gtgj_sql = """
        select count(distinct userid) from (
        select A_table.userid from (
        select distinct(userid) userid from fly_userfocus_tbl
        where createtime between to_date(:start_date, 'yyyy-mm-dd')
        and to_date(:end_date, 'yyyy-mm-dd') and userid like 'gt%' and ordertype = 0
        UNION
        select distinct(token) as userid from fly_userfocus_tbl where createtime
        between to_date(:start_date, 'yyyy-mm-dd')
        and to_date(:end_date, 'yyyy-mm-dd') and platform = 'gtgj')
        A_table
        union
        select B_table.userid  from (
        select distinct(userid) userid from fly_userfocus_tbl_his
        where createtime between to_date(:start_date, 'yyyy-mm-dd')
        and to_date(:end_date, 'yyyy-mm-dd') and userid like 'gt%' and ordertype = 0
        UNION
        select distinct(token) as userid from FLY_USERFOCUS_TBL_HIS where createtime
        between to_date(:start_date, 'yyyy-mm-dd')
        and to_date(:end_date, 'yyyy-mm-dd') and platform = 'gtgj'
        ) B_table
        )
    """

    gtgj_sql_pv = """
        select sum(count_pv) from (
        select sum(a_table.pv) count_pv from (
        select count(*) pv from fly_userfocus_tbl
        where createtime between to_date(:start_date, 'yyyy-mm-dd')
        and to_date(:end_date, 'yyyy-mm-dd') and userid like 'gt%' and ordertype = 0
        union ALL
        select count(*) as pv from fly_userfocus_tbl where createtime
        between to_date(:start_date, 'yyyy-mm-dd')
        and to_date(:end_date, 'yyyy-mm-dd') and platform = 'gtgj') a_table
        union all
        select sum(b_table.pv) count_pv from (
                select count(*) pv from fly_userfocus_tbl_his
                where createtime between to_date(:start_date, 'yyyy-mm-dd')
                and to_date(:end_date, 'yyyy-mm-dd') and userid like 'gt%' and ordertype = 0
                union ALL
                select count(*) as pv from fly_userfocus_tbl_his where createtime
                between to_date(:start_date, 'yyyy-mm-dd')
                and to_date(:end_date, 'yyyy-mm-dd') and platform = 'gtgj'
        ) b_table
        )
    """

    jieji_sql = """
    select count(distinct uv) from (
        select distinct(token) as uv from fly_userfocus_tbl
        where createtime between to_date(:start_date, 'yyyy-mm-dd')
        and to_date(:end_date, 'yyyy-mm-dd') and ordertype = 0 and platform = 'jieji'
        union
        select distinct(token) as uv from fly_userfocus_tbl_his
        where createtime between to_date(:start_date, 'yyyy-mm-dd')
        and to_date(:end_date, 'yyyy-mm-dd') and ordertype = 0 and platform = 'jieji')
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

    android_uv = iphone_uv = weixin_uv = jieji_uv = duanxin_uv = gtgj_uv = total_uv = weixin_applate_uv = 0
    android_pv = iphone_pv = weixin_pv = jieji_pv = duanxin_pv = gtgj_pv = total_pv = weixin_applate_pv = 0
    start_date = DateUtil.get_date_before_days(int(days))
    end_date = DateUtil.get_date_after_days(1-int(days))
    dto = {"start_date": DateUtil.date2str(start_date, '%Y-%m-%d'), "end_date": DateUtil.date2str(end_date, '%Y-%m-%d')}
    app_data_uv = DBCli().oracle_cli.queryAll(all_platform_sql_uv, dto)
    for app in app_data_uv:
        platform, app_uv = app[0], app[1]
        if platform in ['iphone', 'iphonepro']:
            iphone_uv += app_uv
        elif platform == 'android':
            android_uv += app_uv
        elif platform == 'weixin':
            weixin_uv += app_uv
        else:
            weixin_applate_uv += app_uv

    jieji_data = DBCli().oracle_cli.queryOne(jieji_sql, dto)
    duanxin_data = DBCli().oracle_cli.queryOne(duanxin_sql, dto)
    jieji_uv = jieji_data[0]
    duanxin_uv = duanxin_data[0]
    gtgj_data = DBCli().oracle_cli.queryOne(gtgj_sql, dto)
    gtgj_uv = gtgj_data[0]

    total_uv = iphone_uv + android_uv + weixin_uv + gtgj_uv + jieji_uv + duanxin_uv + weixin_applate_uv

    app_data_pv = DBCli().oracle_cli.queryAll(pv_sql, dto)
    for app in app_data_pv:
        platform, app_pv = app[0], app[1]
        if platform in ['iphone', 'iphonepro']:
            iphone_pv += app_pv
        elif platform == 'android':
            android_pv += app_pv
        elif platform == 'weixin':
            weixin_pv += app_pv
        else:
            weixin_applate_pv += app_pv

    app_data_pv = DBCli().oracle_cli.queryAll(pv_his_sql, dto)
    for app in app_data_pv:
        platform, app_pv = app[0], app[1]
        if platform in ['iphone', 'iphonepro']:
            iphone_pv += app_pv
        elif platform == 'android':
            android_pv += app_pv
        elif platform == 'weixin':
            weixin_pv += app_pv
        else:
            weixin_applate_pv += app_pv

    jieji_data_pv = DBCli().oracle_cli.queryOne(jieji_sql_pv, dto)
    duanxin_data_pv = DBCli().oracle_cli.queryOne(duanxin_sql_pv, dto)
    jieji_pv = jieji_data_pv[0]
    duanxin_pv = duanxin_data_pv[0]
    gtgj_data_pv = DBCli().oracle_cli.queryOne(gtgj_sql_pv, dto)
    gtgj_pv = gtgj_data_pv[0]
    total_pv = iphone_pv + android_pv + weixin_pv + gtgj_pv + jieji_pv + duanxin_pv + weixin_applate_pv

    insert_sql = """
        insert into hbdt_focus_platform_daily (s_day, android_uv, iphone_uv, weixin_uv, gtgj_uv,
        jieji_uv, sms_uv, weixin_applet_uv, uv, android_pv, iphone_pv, weixin_pv, gtgj_pv, jieji_pv, sms_pv,
        weixin_applet_pv, pv,
        createtime, updatetime) values
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now() ,
        s_day = VALUES(s_day),
        android_uv = VALUES(android_uv),
        iphone_uv = VALUES(iphone_uv),
        weixin_uv = VALUES(weixin_uv),
        gtgj_uv = VALUES(gtgj_uv),
        jieji_uv = VALUES(jieji_uv),
        sms_uv = VALUES(sms_uv),
        weixin_applet_uv = VALUES(weixin_applet_uv),
        uv = VALUES(uv),
        android_pv = VALUES(android_pv),
        iphone_pv = VALUES(iphone_pv),
        weixin_pv = VALUES(weixin_pv),
        gtgj_pv = VALUES(gtgj_pv),
        jieji_pv = VALUES(jieji_pv),
        sms_pv = VALUES(sms_pv),
        weixin_applet_pv = VALUES(weixin_applet_pv),
        pv = VALUES(pv)
    """

    result_data = [ DateUtil.date2str(start_date, '%Y-%m-%d'), str(android_uv), str(iphone_uv), str(weixin_uv),
                    str(gtgj_uv), str(jieji_uv), str(duanxin_uv), str(weixin_applate_uv), str(total_uv), str(android_pv), str(iphone_pv),
                    str(weixin_pv), str(gtgj_pv), str(jieji_pv), str(duanxin_pv), str(weixin_applate_pv), str(total_pv)]
    DBCli().targetdb_cli.insert(insert_sql, result_data)


def update_focus_platform_weekly():

    all_platform_sql_uv = """
        select platform, count(distinct userid) from (
            select distinct(userid) userid, platform from fly_userfocus_tbl
            where createtime between to_date(:start_date, 'yyyy-mm-dd') and to_date(:end_date, 'yyyy-mm-dd')
            and ordertype = 0 and (platform = 'android' or platform = 'iphone'
            or platform = 'iphonepro' or platform = 'web' or platform='weixin')
            and userid not like 'gt%'
            union
            select distinct(userid) userid, platform from fly_userfocus_tbl_his
            where createtime between to_date(:start_date, 'yyyy-mm-dd') and to_date(:end_date, 'yyyy-mm-dd')
            and ordertype = 0 and (platform = 'android' or platform = 'iphone'
            or platform = 'iphonepro' or platform = 'web' or platform='weixin')
            and userid not like 'gt%') group by platform
    """

    gtgj_sql = """
    select count(distinct userid) from (
        select distinct(userid) userid from fly_userfocus_tbl
        where createtime between to_date(:start_date, 'yyyy-mm-dd')
        and to_date(:end_date, 'yyyy-mm-dd') and userid like 'gt%' and ordertype = 0
        union
        select distinct(userid) userid from fly_userfocus_tbl_his
        where createtime between to_date(:start_date, 'yyyy-mm-dd')
        and to_date(:end_date, 'yyyy-mm-dd') and userid like 'gt%' and ordertype = 0)
    """

    jieji_sql = """
    select count(distinct uv) from (
        select distinct(token) as uv from fly_userfocus_tbl
        where createtime between to_date(:start_date, 'yyyy-mm-dd')
        and to_date(:end_date, 'yyyy-mm-dd') and ordertype = 0 and platform = 'jieji'
        union
        select distinct(token) as uv from fly_userfocus_tbl_his
        where createtime between to_date(:start_date, 'yyyy-mm-dd')
        and to_date(:end_date, 'yyyy-mm-dd') and ordertype = 0 and platform = 'jieji')
    """

    sms_sql = """
            select count(distinct phone) from (
                select distinct phone from fly_userfocus_tbl
                where createtime between to_date(:start_date, 'yyyy-mm-dd')
                and to_date(:end_date, 'yyyy-mm-dd') and ordertype = 1
                union
                select distinct phone from FLY_USERFOCUS_TBL_HIS
                where createtime between to_date(:start_date, 'yyyy-mm-dd')
                and to_date(:end_date, 'yyyy-mm-dd') and ordertype = 1)
                """

    android_uv = iphone_uv = weixin_uv = jieji_uv \
        = gtgj_uv = total_uv = weixin_applate_uv = sms_uv = 0

    start_date, end_date = DateUtil.get_last_week_date()

    dto = {"start_date": DateUtil.date2str(start_date, '%Y-%m-%d'), "end_date": DateUtil.date2str(end_date, '%Y-%m-%d')}
    app_data_uv = DBCli().oracle_cli.queryAll(all_platform_sql_uv, dto)
    for app in app_data_uv:
        platform, app_uv = app[0], app[1]
        if platform in ['iphone', 'iphonepro']:
            iphone_uv += app_uv
        elif platform == 'android':
            android_uv += app_uv
        elif platform == 'weixin':
            weixin_uv += app_uv
        else:
            weixin_applate_uv += app_uv

    jieji_data = DBCli().oracle_cli.queryOne(jieji_sql, dto)
    jieji_uv = jieji_data[0]
    gtgj_data = DBCli().oracle_cli.queryOne(gtgj_sql, dto)
    gtgj_uv = gtgj_data[0]

    sms_uv = (DBCli().oracle_cli.queryOne(sms_sql, dto))[0]

    total_uv = iphone_uv + android_uv + weixin_uv + gtgj_uv + jieji_uv + weixin_applate_uv + sms_uv

    insert_sql = """
        insert into hbdt_focus_platform_weekly (s_day, android_uv, iphone_uv, weixin_uv, gtgj_uv,
        jieji_uv, sms_uv, weixin_applet_uv, uv,
        createtime, updatetime) values
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now() ,
        s_day = VALUES(s_day),
        android_uv = VALUES(android_uv),
        iphone_uv = VALUES(iphone_uv),
        weixin_uv = VALUES(weixin_uv),
        gtgj_uv = VALUES(gtgj_uv),
        jieji_uv = VALUES(jieji_uv),
        sms_uv = VALUES(sms_uv),
        weixin_applet_uv = VALUES(weixin_applet_uv),
        uv = VALUES(uv)
    """

    result_data = [ DateUtil.date2str(start_date, '%Y-%m-%d'), str(android_uv), str(iphone_uv), str(weixin_uv),
                    str(gtgj_uv), str(jieji_uv), str(sms_uv), str(weixin_applate_uv), str(total_uv)]
    DBCli().targetdb_cli.insert(insert_sql, result_data)


def update_focus_platform_monthly():

    all_platform_sql_uv = """
        select platform, count(distinct userid) from (
            select distinct(userid) userid, platform from fly_userfocus_tbl
            where createtime between to_date(:start_date, 'yyyy-mm-dd') and to_date(:end_date, 'yyyy-mm-dd')
            and ordertype = 0 and (platform = 'android' or platform = 'iphone'
            or platform = 'iphonepro' or platform = 'web' or platform='weixin')
            and userid not like 'gt%'
            union
            select distinct(userid) userid, platform from fly_userfocus_tbl_his
            where createtime between to_date(:start_date, 'yyyy-mm-dd') and to_date(:end_date, 'yyyy-mm-dd')
            and ordertype = 0 and (platform = 'android' or platform = 'iphone'
            or platform = 'iphonepro' or platform = 'web' or platform='weixin')
            and userid not like 'gt%') group by platform
    """

    gtgj_sql = """
    select count(distinct userid) from (
        select distinct(userid) userid from fly_userfocus_tbl
        where createtime between to_date(:start_date, 'yyyy-mm-dd')
        and to_date(:end_date, 'yyyy-mm-dd') and userid like 'gt%' and ordertype = 0
        union
        select distinct(userid) userid from fly_userfocus_tbl_his
        where createtime between to_date(:start_date, 'yyyy-mm-dd')
        and to_date(:end_date, 'yyyy-mm-dd') and userid like 'gt%' and ordertype = 0)
    """

    jieji_sql = """
    select count(distinct uv) from (
        select distinct(token) as uv from fly_userfocus_tbl
        where createtime between to_date(:start_date, 'yyyy-mm-dd')
        and to_date(:end_date, 'yyyy-mm-dd') and ordertype = 0 and platform = 'jieji'
        union
        select distinct(token) as uv from fly_userfocus_tbl_his
        where createtime between to_date(:start_date, 'yyyy-mm-dd')
        and to_date(:end_date, 'yyyy-mm-dd') and ordertype = 0 and platform = 'jieji')
    """

    sms_sql = """
            select count(distinct phone) from (
                select distinct phone from fly_userfocus_tbl
                where createtime between to_date(:start_date, 'yyyy-mm-dd')
                and to_date(:end_date, 'yyyy-mm-dd') and ordertype = 1
                union
                select distinct phone from FLY_USERFOCUS_TBL_HIS
                where createtime between to_date(:start_date, 'yyyy-mm-dd')
                and to_date(:end_date, 'yyyy-mm-dd') and ordertype = 1) """

    android_uv = iphone_uv = weixin_uv = jieji_uv \
        = gtgj_uv = total_uv = weixin_applate_uv = sms_uv = 0

    start_date, end_date = DateUtil.get_last_month_date()
    dto = {"start_date": DateUtil.date2str(start_date, '%Y-%m-%d'), "end_date": DateUtil.date2str(end_date, '%Y-%m-%d')}
    app_data_uv = DBCli().oracle_cli.queryAll(all_platform_sql_uv, dto)
    for app in app_data_uv:
        platform, app_uv = app[0], app[1]
        if platform in ['iphone', 'iphonepro']:
            iphone_uv += app_uv
        elif platform == 'android':
            android_uv += app_uv
        elif platform == 'weixin':
            weixin_uv += app_uv
        else:
            weixin_applate_uv += app_uv

    jieji_data = DBCli().oracle_cli.queryOne(jieji_sql, dto)
    jieji_uv = jieji_data[0]
    gtgj_data = DBCli().oracle_cli.queryOne(gtgj_sql, dto)
    gtgj_uv = gtgj_data[0]

    sms_uv = (DBCli().oracle_cli.queryOne(sms_sql, dto))[0]

    total_uv = iphone_uv + android_uv + weixin_uv + gtgj_uv + jieji_uv + weixin_applate_uv + sms_uv

    insert_sql = """
        insert into hbdt_focus_platform_monthly (s_day, android_uv, iphone_uv, weixin_uv, gtgj_uv,
        jieji_uv, sms_uv, weixin_applet_uv, uv,
        createtime, updatetime) values
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on duplicate key update updatetime = now() ,
        s_day = VALUES(s_day),
        android_uv = VALUES(android_uv),
        iphone_uv = VALUES(iphone_uv),
        weixin_uv = VALUES(weixin_uv),
        gtgj_uv = VALUES(gtgj_uv),
        jieji_uv = VALUES(jieji_uv),
        sms_uv = VALUES(sms_uv),
        weixin_applet_uv = VALUES(weixin_applet_uv),
        uv = VALUES(uv)
    """

    result_data = [ DateUtil.date2str(start_date, '%Y-%m-%d'), str(android_uv), str(iphone_uv), str(weixin_uv),
                    str(gtgj_uv), str(jieji_uv), str(sms_uv), str(weixin_applate_uv), str(total_uv)]
    DBCli().targetdb_cli.insert(insert_sql, result_data)

if __name__ == "__main__":
    # one_focus = open("new_one_focus.dat", 'a')
    # import datetime
    # start_date = datetime.date(2017, 1, 1)
    # end_date = datetime.date(2017, 1, 22)
    # while start_date < end_date:
    #     next_day = DateUtil.add_days(start_date, 1)
    #     out_str = update_focus_platform(start_date, next_day)
    #     start_date = DateUtil.add_days(start_date, 1)
    #     one_focus.write(out_str + "\n")
    # one_focus.close()
    # update_focus_platform(1)
    # import datetime
    # end = datetime.date(2017, 2, 6)
    # # start_date, end_date = DateUtil.get_last_week_date(end)
    # # print start_date, end_date
    # # update_focus_platform_weekly(start_date, end_date)
    # start_date, end_date = DateUtil.get_last_month_date()
    # print start_date, end_date
    update_focus_platform(1)
    # i = 9
    # while i >= 1:
    #     update_focus_platform(i)
    #     i -= 1

