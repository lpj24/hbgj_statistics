from dbClient.dbUtils import DButils
import conf


class DBCli(object):

    def __init__(self, *args):
        self._source_db = conf.source_db
        self._target_db = conf.target_db
        self._ApiLog_db = conf.ApiLog_db
        self._oracle_db = conf.oracle_db
        self._car_db = conf.car_db
        self._huoli_db = conf.huoli_db
        self._gt_db = conf.gt_db
        self._redis_db = conf.redis_db
        self._sky_hotel = conf.sky_hotel
        self._target_db_test = conf.target_db_test
        self._hb_fly = conf.hb_fly,
        self._pay_cost = conf.pay_cost
        self._hb_partner = conf.hb_partner
        self._tongji_skyhotel = conf.tongji_skyhotel
        self._source_account_db = conf.source_account_db
        self._dynamic_focus = conf.dynamic_focus
        self._apibase = conf.hb_apibase
        self.args = args if args else (list, )

    @property
    def sourcedb_cli(self):
        return DButils("mysql", self._source_db, self.args[0])
        # return DButils("mysql", self._source_db)

    @property
    def targetdb_cli(self):
        return DButils("mysql", self._target_db)

    @property
    def Apilog_cli(self):
        return DButils("mysql", self._ApiLog_db)

    @property
    def oracle_cli(self):
        return DButils("oracle", self._oracle_db)

    @property
    def huoli_cli(self):
        return DButils("mysql", self._huoli_db)

    @property
    def car_cli(self):
        return DButils("mysql", self._car_db)

    @property
    def gt_cli(self):
        """
        Usage::
            >>> return_data = DBCli(dict).gt_cli.queryOne(sql, dto)
            >>> print type(return_data)
            <type 'dict'>
            >>> return_data = DBCli().gt_cli.queryOne(sql, dto)
            >>> print type(return_data)
              <type 'list'>
        """
        return DButils("mysql", self._gt_db, self.args[0])

    @property
    def redis_cli(self):
        return conf.redis_cli

    @property
    def redis_dt_cli(self):
        return conf.redis_hbdt_cli

    @property
    def sky_hotel_cli(self):
        return DButils("mysql", self._sky_hotel)

    @property
    def targetdb_cli_test(self):
        return DButils("mysql", self._target_db_test)

    @property
    def hb_fly_cli(self):
        return DButils("oracle", self._hb_fly)

    @property
    def pay_cost_cli(self):
        # return DButils("mysql", self._pay_cost)
        return DButils("mysql", self._pay_cost, self.args[0])

    @property
    def hb_partner_cli(self):
        return DButils("mysql", self._hb_partner)

    @property
    def hb_source_account_cli(self):
        return DButils("mysql", self._source_account_db)

    @property
    def hb_sky_account_cli(self):
        new_account = self._source_account_db
        new_account["database"] = "account"
        return DButils("mysql", new_account)

    @property
    def tongji_skyhotel_cli(self):
        return DButils("mysql", self._tongji_skyhotel)

    @property
    def dynamic_focus_cli(self):
        return DButils("mysql", self._dynamic_focus)

    @property
    def apibase_cli(self):
        return DButils("mysql", self._apibase)


if __name__ == "__main__":
    import logging
    logging