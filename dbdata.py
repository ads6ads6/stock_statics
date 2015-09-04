import tushare
import MySQLdb
from datetime import datetime
from map_code import map_code


class Dbbase(object):
    def __init__(self, code):
        self.conn = MySQLdb.connect(host='120.26.211.94', user='root', passwd='autott', db='test')
        self.cursor = self.conn.cursor()
        self.execute = self.cursor.execute
        self.code = code

class Dboperation(Dbbase):
    def __init__(self, code):
        super(Dboperation, self).__init__(code)
        self.exist = None

    def initialize(self):
        if not self.code.startswith('b'):
            self.code = "`{}`".format(self.code)
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

    def get_start_date(self):
        self.execute('select date from {} order by date limit 1'.format(self.code))
        return self.cursor.fetchall()[0][0]


if __name__ == '__main__':
    #data = Dboperation('b000001')
    #data.initialize()
    for code in map_code:
        data = Dboperation(code)
        data.initialize()
    #print data.get_latest_date()
    #print data.get_start_date
