import tushare as ts
from dboperation import Dboperation
import datetime
from sqlalchemy import create_engine
from map_code import code_list, market_index_list
from dbinfo import dbinfo


engine = create_engine('mysql://{}:{}@{}/test?charset=utf8'.format(dbinfo['USER'], dbinfo['PASSWORD'], dbinfo['HOST_IP']))

class Updatedb(object):
    def __init__(self, db, code):
        self.db = db
        self.code = code
        self.db.initialize(self.code)

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
        df = self._get_ts_data()
        if df is None:
            return
        df.to_sql(self.code, engine, if_exists='append')



if __name__ == '__main__':
    db = Dboperation()
    for code in market_index_list.keys() + code_list.keys():
        update = Updatedb(db, code)
        update.append_to_db()

    db_init = Dboperation()
    for code in market_index_list.keys() + code_list.keys():
        db_init.initialize(code)

