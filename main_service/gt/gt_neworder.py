from sql.gt_sqlHandlers import gt_new_order_sql
from dbClient.db_client import DBCli
from dbClient.dateutil import DateUtil


def update_gt_order_daily(days=0):
    if days > 0:
        start_date = DateUtil.get_date_before_days(25)
        end_date = DateUtil.get_date_after_days(-7)
    else:
        start_date = DateUtil.get_date_before_days(days)
        end_date = DateUtil.add_days(start_date, 1)
    dto = []
    for x in xrange(2):
        dto.append(DateUtil.date2str(start_date))
        dto.append(DateUtil.date2str(end_date))
    print dto
    query_data = DBCli().gt_cli.query_all(gt_new_order_sql["gt_neworder_daily"], dto)
    DBCli().targetdb_cli.batch_insert(gt_new_order_sql["update_gtgj_new_order_daily"], query_data)

if __name__ == "__main__":
    update_gt_order_daily(1)
    # update_gt_order_hourly()
