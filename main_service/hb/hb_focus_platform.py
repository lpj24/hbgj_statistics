from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_focus_platform():
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
        union
        SELECT phoneid FROM FLY_USERFOCUS_TBL
        where PHONEID>0
        and PLATFORM = 'gtgj'
        and CREATETIME<to_date(:end_date, 'yyyy-mm-dd')
        and CREATETIME>=to_date(:start_date, 'yyyy-mm-dd')
        union
        SELECT phoneid FROM FLY_USERFOCUS_TBL_HIS
        where PHONEID>0
        and PLATFORM = 'gtgj'
        and CREATETIME<to_date(:end_date, 'yyyy-mm-dd')
        and CREATETIME>=to_date(:start_date, 'yyyy-mm-dd')
        union
        SELECT userid phoneid FROM FLY_USERFOCUS_TBL
        where PHONEID=0
        and PLATFORM = 'gtgj'
        and CREATETIME<to_date(:end_date, 'yyyy-mm-dd')
        and CREATETIME>=to_date(:start_date, 'yyyy-mm-dd')

        union
        SELECT userid phoneid FROM FLY_USERFOCUS_TBL_HIS
        where PHONEID=0
        and PLATFORM = 'gtgj'
        and CREATETIME<to_date(:end_date, 'yyyy-mm-dd')
        and CREATETIME>=to_date(:start_date, 'yyyy-mm-dd')
    )
    """

    gtgj_other_sql = """

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
    start_date = datetime.date(2017, 1, 1)
    end_date = datetime.date(2017, 1, 2)
    dto = {"start_date": DateUtil.date2str(start_date, '%Y-%m-%d'), "end_date": DateUtil.date2str(end_date, '%Y-%m-%d')}
    gtgj_data_uv = DBCli().oracle_cli.queryOne(gtgj_sql, dto)
    print gtgj_data_uv

if __name__ == "__main__":
    update_focus_platform()