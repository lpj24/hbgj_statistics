from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_focus_platform(start_date, end_date):
    all_platform_sql_uv = """
        select platform, sum(focus_users) from (
        select count(DISTINCT phoneid) focus_users, platform from (
            SELECT phoneid, platform FROM FLY_USERFOCUS_TBL
            where PHONEID>0
            and CREATETIME<to_date(:end_date, 'yyyy-mm-dd')
            and CREATETIME>=to_date(:start_date, 'yyyy-mm-dd')
            and ordertype = 0
            and platform in ('android', 'weixin', 'iphone', 'iphonepro')
            union
            SELECT phoneid, platform FROM FLY_USERFOCUS_TBL_HIS
            where PHONEID>0
            and ordertype = 0
            and platform in ('android', 'weixin', 'iphone', 'iphonepro')
            and CREATETIME<to_date(:end_date, 'yyyy-mm-dd')
            and CREATETIME>=to_date(:start_date, 'yyyy-mm-dd')
        ) GROUP BY platform
        UNION
        select count(DISTINCT userid) focus_users, platform from (
            SELECT userid, platform from FLY_USERFOCUS_TBL
            where PHONEID=0
            and CREATETIME<to_date(:end_date, 'yyyy-mm-dd')
            and CREATETIME>=to_date(:start_date, 'yyyy-mm-dd')
            and ordertype = 0
            and platform in ('android', 'weixin', 'iphone', 'iphonepro')
            UNION
            SELECT USERID, platform from FLY_USERFOCUS_TBL_HIS
            where PHONEID=0
            and ordertype = 0
            and platform in ('android', 'weixin', 'iphone', 'iphonepro')
            and CREATETIME<to_date(:end_date, 'yyyy-mm-dd')
            and CREATETIME>=to_date(:start_date, 'yyyy-mm-dd')
        ) GROUP BY platform ) GROUP BY platform
    """

    pv_sql = """
        SELECT platform, count(*) FROM FLY_USERFOCUS_TBL
        where CREATETIME<to_date(:end_date, 'yyyy-mm-dd')
        and CREATETIME>=to_date(:start_date, 'yyyy-mm-dd')
        and ordertype = 0
        and platform in ('android', 'weixin', 'iphone', 'iphonepro')
        group by platform
    """
    pv_his_sql = """
        SELECT platform, count(*) FROM FLY_USERFOCUS_TBL_HIS
        where CREATETIME<to_date(:end_date, 'yyyy-mm-dd')
        and CREATETIME>=to_date(:start_date, 'yyyy-mm-dd')
        and ordertype = 0
        and platform in ('android', 'weixin', 'iphone', 'iphonepro')
        group by platform
    """

    all_uv_sql = """
            select :s_day s_day,sum(focus_users) from (
    select count(DISTINCT phoneid) focus_users from (
        SELECT phoneid FROM FLY_USERFOCUS_TBL
        where PHONEID>0
        and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
        and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
        union
        SELECT phoneid FROM FLY_USERFOCUS_TBL_HIS
        where PHONEID>0
        and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
        and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
        )
        UNION
        select count(DISTINCT userid) focus_users from (
            SELECT userid from FLY_USERFOCUS_TBL
            where PHONEID=0
            and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
            and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
            UNION
            SELECT USERID from FLY_USERFOCUS_TBL_HIS
            where PHONEID=0
            and CREATETIME<to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS')
            and CREATETIME>=to_date(:start_date, 'YYYY-MM-DD HH24:MI:SS')
        ) )
    """
    # all_other_sql_pv = """
    #         SELECT count(*) FROM FLY_USERFOCUS_TBL
    #         where CREATETIME<to_date(:end_date, 'yyyy-mm-dd')
    #         and CREATETIME>=to_date(:start_date, 'yyyy-mm-dd')
    #     """
    #
    # all_other_sql_pv_his = """
    #     SELECT count(*) FROM FLY_USERFOCUS_TBL_HIS
    #     where CREATETIME<to_date(:end_date, 'yyyy-mm-dd')
    #     and CREATETIME>=to_date(:start_date, 'yyyy-mm-dd')
    # """

    gtgj_sql = """
        select sum(uv) from (
        select count(distinct(userid)) uv
        from fly_userfocus_tbl where createtime
        between to_date(:start_date, 'yyyy-mm-dd') and to_date
        (:end_date, 'yyyy-mm-dd') and userid like 'gt%'
        union
        select count(distinct(token)) uv from fly_userfocus_tbl_his
        where createtime
        between to_date(:start_date, 'yyyy-mm-dd') and to_date(:end_date, 'yyyy-mm-dd') and platform = 'gtgj')
    """

    gtgj_sql_pv = """
        select sum(pv) from (
        select count(*) pv
        from fly_userfocus_tbl where createtime
        between to_date(:start_date, 'yyyy-mm-dd') and to_date
        (:end_date, 'yyyy-mm-dd') and userid like 'gt%'
        union
        select count(*) pv from fly_userfocus_tbl_his
        where createtime
        between to_date(:start_date, 'yyyy-mm-dd') and to_date(:end_date, 'yyyy-mm-dd') and platform = 'gtgj')
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

    android_uv = iphone_uv = weixin_uv = jieji_uv = duanxin_uv = gtgj_uv = total_uv = total_uv_all = 0
    android_pv = iphone_pv = weixin_pv = jieji_pv = duanxin_pv = gtgj_pv = total_pv = 0
    dto = {"start_date": DateUtil.date2str(start_date, '%Y-%m-%d'), "end_date": DateUtil.date2str(end_date, '%Y-%m-%d')}
    app_data_uv = DBCli().oracle_cli.queryAll(all_platform_sql_uv, dto)
    for app in app_data_uv:
        platform, app_uv = app[0], app[1]
        if platform in ['iphone', 'iphonepro']:
            iphone_uv += app_uv
        elif platform == 'android':
            android_uv += app_uv
        else:
            weixin_uv += app_uv

    jieji_data = DBCli().oracle_cli.queryOne(jieji_sql, dto)
    duanxin_data = DBCli().oracle_cli.queryOne(duanxin_sql, dto)
    jieji_uv = jieji_data[0]
    duanxin_uv = duanxin_data[0]
    gtgj_data = DBCli().oracle_cli.queryOne(gtgj_sql, dto)
    gtgj_uv = gtgj_data[0]

    total_uv = iphone_uv + android_uv + weixin_uv + gtgj_uv + jieji_uv + duanxin_uv

    app_data_pv = DBCli().oracle_cli.queryAll(pv_sql, dto)
    for app in app_data_pv:
        platform, app_pv = app[0], app[1]
        if platform in ['iphone', 'iphonepro']:
            iphone_pv += app_pv
        elif platform == 'android':
            android_pv += app_pv
        else:
            weixin_pv += app_pv

    app_data_pv = DBCli().oracle_cli.queryAll(pv_his_sql, dto)
    for app in app_data_pv:
        platform, app_pv = app[0], app[1]
        if platform in ['iphone', 'iphonepro']:
            iphone_pv += app_pv
        elif platform == 'android':
            android_pv += app_pv
        else:
            weixin_pv += app_pv

    jieji_data_pv = DBCli().oracle_cli.queryOne(jieji_sql_pv, dto)
    duanxin_data_pv = DBCli().oracle_cli.queryOne(duanxin_sql_pv, dto)
    jieji_pv = jieji_data_pv[0]
    duanxin_pv = duanxin_data_pv[0]
    gtgj_data_pv = DBCli().oracle_cli.queryOne(gtgj_sql_pv, dto)
    gtgj_pv = gtgj_data_pv[0]
    total_pv = iphone_pv + android_pv + weixin_pv + gtgj_pv + jieji_pv + duanxin_pv

    # check_data_uv = DBCli().oracle_cli.queryOne(all_other_sql_pv, dto)
    # check_data_his_uv = DBCli().oracle_cli.queryOne(all_other_sql_pv_his, dto)
    # total_uv_all = check_data_uv[0] + check_data_his_uv[0]

    out_str = DateUtil.date2str(start_date, '%Y-%m-%d') + "\t" + str(android_uv) + "\t" \
              + str(iphone_uv) + "\t" + str(weixin_uv) + "\t" + str(gtgj_uv) + \
              "\t" + str(jieji_uv) + "\t" + str(duanxin_uv) + "\t" + str(total_uv) + "\t" +  \
              "\t" + str(android_pv) + "\t" + str(iphone_pv) + "\t" + str(weixin_pv) + "\t" + str(gtgj_pv) + \
              "\t" + str(jieji_pv) + "\t" + str(duanxin_pv) + "\t" + str(total_pv)
    print out_str
    return out_str


if __name__ == "__main__":
    one_focus = open("new_one_focus.dat", 'a')
    import datetime
    start_date = datetime.date(2017, 1, 1)
    end_date = datetime.date(2017, 1, 22)
    while start_date < end_date:
        next_day = DateUtil.add_days(start_date, 1)
        out_str = update_focus_platform(start_date, next_day)
        start_date = DateUtil.add_days(start_date, 1)
        one_focus.write(out_str + "\n")
    one_focus.close()
