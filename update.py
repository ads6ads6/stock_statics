import tushare as ts
from dbdata import Dboperation
import datetime
from sqlalchemy import create_engine
from map_code import map_code


Market_index_list = ['b000001', 'b399001', 'b399005', 'b399006', 'b000016', 'b000300']

class Updatedb(object):
    def __init__(self, code):
        self.code = code
        self.db = Dboperation(self.code)
        self.db.initialize()
        self.engine = create_engine('mysql://root:autott@120.26.211.94/test?charset=utf8')

    def date_start_update(self):
        latest = self.db.get_latest_date()
        now = latest + datetime.timedelta(1)
        return str(now)

    def get_ts_data(self):
        if self.code in Market_index_list:
            return ts.get_h_data(code=self.code[1:], index=True, start=self.date_start_update())
        else:
            return ts.get_h_data(code=self.code, autype='hfq', start=self.date_start_update())

    def append_to_db(self):
        df = self.get_ts_data()
        if df is None:
            return
        df.to_sql(self.code, self.engine, if_exists='append')
        self.db.sort_by_date()
        if not self.db.exist:
            self.db.exist = True
            self.db.set_date_type()
            self.db.set_pri_key()



if __name__ == '__main__':
    for code in Market_index_list + map_code.keys():
        update = Updatedb(code)
        update.append_to_db()


