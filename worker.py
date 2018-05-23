# -*- coding: utf-8 -*-
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


if __name__ == '__main__':
    company_sql = """
        select name from company_user
    """

    sql = """
        SELECT o.PHONEID, pu.`NAME`, pu.PHONE, sum(od.OUTPAYPRICE), count(o.ORDERID) from TICKET_ORDER o 
        LEFT JOIN phone_user pu on o.PHONEID=pu.phoneid
        LEFT JOIN TICKET_ORDERDETAIL od on o.ORDERID=od.ORDERID
        where od.CREATETIME>='2017-05-01' and od.CREATETIME<'2018-05-01'
        and o.ORDERSTATUE NOT IN (0, 1, 11, 12, 2, 21, 3, 31) 
        AND IFNULL(od.`LINKTYPE`, 0) != 2
        and o.PHONEID in %s
        group by o.PHONEID, pu.`NAME`, pu.PHONE
            
    """

    import pandas as pd
    book = pd.read_excel("C:\\Users\\Administrator\\Documents\\Tencent Files\\762575190\\FileRecv\\haidan.xlsx", sheetname='Sheet0')
    search_phoneid = [i for i in book['phoneid']]
    # search_uid = [i for i in book['id']]
    print search_phoneid
    company_users = DBCli().huoli_buy_cli.query_all(company_sql)
    company_users = [c[0] for c in company_users]
    consumers_list = DBCli().sourcedb_cli.query_all(sql, [[25298457, 45567269, 12097757, 45564364, 21820, 26174759, 25141084, 22585037, 25288114, 31494219, 44308012, 31516997, 26343403, 29668867, 29924411, 28547586, 29677404, 45222803, 42144295, 45510318, 26604542, 44075565, 26649497, 27013643, 44080429, 27743528, 27981655, 27337683, 6413429, 45505687, 31496367, 27261756, 8936315, 26713124, 42379599, 5175995, 45498576, 41906663, 24567080, 11450219, 30804847, 31109514, 25298457, 24326, 16401848, 38533293]
])
    print len(consumers_list)
    from collections import defaultdict
    result = defaultdict(int)
    for c in consumers_list:
        phoneid, name, phone, amount, nums = c
        if name in company_users:
            print str(phoneid) + '\t' + name + '\t' + str(nums) + '\t' + str(amount) + '\t' + '1'
        else:
            print str(phoneid) + '\t' + name + '\t' + str(nums) + '\t' + str(amount) + '\t' + '0'
