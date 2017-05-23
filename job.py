# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
import tushare as ts

if __name__ == "__main__":
    # 偿债能力分析
    df = ts.get_debtpaying_data(2016, 4)

    engine = create_engine('mysql://lpj:123456@127.0.0.1/statistics?charset=utf8')

    # 存入数据库
    df.to_sql('debt_ability', engine, if_exists='append')

    # 追加数据到现有表
    # df.to_sql('tick_data',engine,if_exists='append')