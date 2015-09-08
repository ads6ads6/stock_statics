from dbbase import Dbbase
from datetime import datetime
from map_code import code_list, market_index_list

class Dboperation(Dbbase):
    def __init__(self):
        super(Dboperation, self).__init__()
        self.exist = None
        self.code = None

    def initialize(self, code):
        self.code = code
        if not self.code.startswith('b'):
            self.code = "`{}`".format(code)
        try:
            self.execute("select * from {} limit 1".format(self.code))
        except Exception as e:
            print 'Not exist', e
            self.exist = False
            return
        self.exist = True
        self._create_change_rate_if_not_exist()
        self._set_date_type()
        self._set_pri_key()
        self.calculate_change_rate()

    def _get_desc_result(self, raw):
        self.execute('desc {} {}'.format(self.code, raw))
        return self.cursor.fetchall()

    def _set_date_type(self):
        if not self.exist:
            return
        if self._get_desc_result('date')[0][1] == 'date':
            return
        self.execute('alter table {} modify date date'.format(self.code))
        self.conn.commit()

    def _set_pri_key(self):
        if not self.exist:
            return
        if self._get_desc_result('date')[0][3] == 'PRI':
            return
        self.execute('alter table {} add primary key (date)'.format(self.code))
        self.conn.commit()

    def _create_change_rate_if_not_exist(self):
        if not self.exist:
            return
        self.execute('desc {} change_rate'.format(self.code))
        if not self.cursor.fetchall():
            self.execute('alter table {} add change_rate double after close'.format(self.code))
            self.conn.commit()

    def calculate_change_rate(self):
        if not self.exist:
            return
        self.execute('select date,close from {} where change_rate is null'.format(self.code))
        data_for_calc = self.cursor.fetchall()
        for data in data_for_calc:
            today_close = data[1]
            self.execute("select close from {} where date < '{}' order by date desc limit 1".format(self.code, data[0]))
            result = self.cursor.fetchall()
            if result:
                last_close = result[0][0]
            else:
                last_close = today_close
            self.execute("update {} set change_rate = {:.2f} where date = '{}'".format
                         (self.code, 100 * (today_close/last_close - 1), data[0]))
        self.conn.commit()

    def sort_by_date(self):
        if not self.exist:
            return
        self.execute('alter table {} order by date desc'.format(self.code))
        self.conn.commit()

    def get_latest_date(self):
        if not self.exist:
            return datetime(1990, 01, 01)
        try:
            self.execute("select date from {} order by date desc limit 1".format(self.code))
        except Exception as e:
            print 'Fail to execute SQL', e
            raise
        return self.cursor.fetchall()[0][0]



if __name__ == '__main__':
    dbdata = Dboperation()
    #data = Dboperation('b000001')
    #data.initialize()
    #data.calculate_change_rate()
    for code in market_index_list.keys() + code_list.keys():
        dbdata.initialize(code)

    #print data.get_latest_date()
    #print data.get_start_date
