import datetime

from sqlalchemy import create_engine
import requests

import tushare as ts
from dboperation import Dboperation
from dbstatistic import Dbstatistic, get_market_date, START_DATE
from dbbase import Dbbase
from map_code import code_list, market_index_list
from dbinfo import dbinfo

engine = create_engine('mysql://{}:{}@{}/test?charset=utf8'.format(dbinfo['USER'], dbinfo['PASSWORD'], dbinfo['HOST_IP']))


class Updatedb(object):
    def __init__(self):
        self.db = Dboperation()
        self.code = None

    def _date_start_update(self):
        latest = self.db.get_latest_date()
        now = latest + datetime.timedelta(1)
        return str(now)

    def _get_ts_data(self):
        if self.code in market_index_list:
            return ts.get_h_data(code=self.code[1:], index=True, start=self._date_start_update())
        else:
            return ts.get_h_data(code=self.code, autype='hfq', start=self._date_start_update())

    def append_to_db(self):
        #market_latest_date = get_market_date(1)
        #if self.db.get_latest_date() == market_latest_date:
        #    if self.code != sh:
        #        return
        df = self._get_ts_data()
        if df is None:
            return
        db_data = df.T.to_dict()
        self.db.append_db(db_data)

    def run_update(self):
        for code in market_index_list + code_list.keys():
            self.code = code
            self.db.initialize(code)
            if not self.db.exist:
                self.db.create_table()
            self.append_to_db()
            self.db.initialize(code)



class UpdateFluc(Dbbase):
    def __init__(self):
        super(UpdateFluc, self).__init__()
        self.table = 'fluctuation'
        self.latest_update = ''
        self.dbstatistic = Dbstatistic()
        self.exist = False

    def _check_result(self, code):
        self.execute("select * from {} where code = '{}'".format(self.table, code))
        result = self.cursor.fetchall()
        if not result:
            self.exist = False
            return ''
        self.exist = True
        return result[0][2]

    def update_table(self, code, exist):
        latest_date = get_market_date(1)
        five_days_gao = get_market_date(5)
        twenty_days_ago = get_market_date(20)
        half_years_ago = get_market_date(120)
        year_ago = get_market_date(240)
        fluctuation_lastday = self.dbstatistic.cal_fluc(start=latest_date, end=latest_date)
        fluctuation_lastweek = self.dbstatistic.cal_fluc(start=five_days_gao, end=latest_date)
        fluctuation_lastmonth = self.dbstatistic.cal_fluc(start=twenty_days_ago, end=latest_date)
        fluctuation_halfyear = self.dbstatistic.cal_fluc(start=half_years_ago, end=latest_date)
        fluctuation_lastyear = self.dbstatistic.cal_fluc(start=year_ago, end=latest_date)
        fluctuation_total = self.dbstatistic.cal_fluc(start=START_DATE, end=latest_date)
        end = self.dbstatistic.code_last_trading_day
        if exist:
            self.execute("update fluctuation set end = '{}',".format(end) +
                         "fluctuation_total = {},".format(fluctuation_total) +
                         "fluctuation_lastyear = {},".format(fluctuation_lastyear) +
                         "fluctuation_halfyear= {},".format(fluctuation_halfyear) +
                         "fluctuation_lastmonth = {},".format(fluctuation_lastmonth) +
                         "fluctuation_lastweek = {},".format(fluctuation_lastweek) +
                         "fluctuation_lastday = {} where code = '{}'".format(fluctuation_lastday, code)
                         )
        else:
            self.execute("insert into fluctuation values(" +
                         "'{}', '{}', '{}', {}, {}, {}, {}, {}, {})".
                         format(code, START_DATE, end, fluctuation_total, fluctuation_lastyear,
                                fluctuation_halfyear, fluctuation_lastmonth,
                                fluctuation_lastweek, fluctuation_lastday))
        self.conn.commit()

    def run_update(self):
        for code in market_index_list + code_list.keys():
            self.dbstatistic.initialize(code)
            end_date = self._check_result(code)
            if get_market_date(1) == end_date:
                continue
            self.update_table(code, exist=self.exist)


class UpdateStockBasic(Dbbase):
    def __init__(self):
        super(UpdateStockBasic, self).__init__()
        self.table = 'stock_basic'
        self.exist = True

    def _check_result(self, code):
        self.execute("select * from {} where code = '{}'".format(self.table, code))
        result = self.cursor.fetchall()
        if not result:
            self.exist = False
        else:
            self.exist = True

    def get_pe_value(self, code):
        if code.startswith('6'):
            code = 'sh' + code
        else:
            code = 'sz' + code
        out = requests.get('http://qt.gtimg.cn/q={}'.format(code)).text.split('~')[-11]
        if not out:
            pe = 'null'
        else:
            pe = float(out)
        return pe

    def update_table(self, code):
        pe = self.get_pe_value(code)
        if self.exist:
            self.execute('update stock_basic set pe = {} where code = {}'.format(pe, code))
        else:
            self.execute("insert into stock_basic values('{}',{})".format(code, pe))
        self.conn.commit()

    def run_update(self):
        for code in code_list.keys():
            self._check_result(code)
            self.update_table(code)

class Update_highest(object):
    pass


if __name__ == '__main__':
    update_basic = UpdateStockBasic()
    update_basic.run_update()


    update_db = Updatedb()
    update_db.run_update()

    #for code in market_index_list + code_list.keys():
    #    update = Updatedb(db, code)
    #    update.append_to_db()

    #db_init = Dboperation()
    #for code in market_index_list + code_list.keys():
    #    db_init.initialize(code)

    update_fluc = UpdateFluc()
    update_fluc.run_update()


