from dbbase import  Dbbase
from map_code import code_list, market_index_list

sh = 'b000001'

class Dbstatistic(Dbbase):
    def __init__(self):
        super(Dbstatistic, self).__init__()
        self.code = None

    def initialize(self, code):
        self.code = code
        if not code.startswith('b'):
            self.code = "`{}`".format(code)

    def _get_start_date(self, last_days):
        self.execute('select date from {} order by date desc limit {}'.format(sh, last_days))
        result = self.cursor.fetchall()
        if result:
            return result[-1][0]

    def fluctuation_in_recent_days(self, last_days):
        start_date = self._get_start_date(last_days)
        self.execute("select high,low from {} where date >= '{}'".format(self.code, start_date))
        result = self.cursor.fetchall()
        if not result:
            return 0.0
        high_list, low_list = zip(*result)
        highest = max(high_list)
        lowest = min(low_list)
        return '{:.2f}'.format(100*(highest-lowest)/lowest)

    def change_rate_by_weekday(self, start, end):
        for i in range(2,7):
            self.execute("select sum(rate),count(*) from {} where date > '{}' and date < '{}' and dayofweek(date) = {})




    def rate_st_weekday(self):
        for i in range(2, 7):
            self.execute('select sum(rate),count(*) from {} where date > date(\'{}\') and date < date(\'{}\') and dayofweek(date) = {} and rate >0'
                         .format(self.table, self.start, self.end, i))
            sum_inc, counter_inc = self.cursor.fetchall()[0]
            self.execute('select sum(rate),count(*) from {} where date > date(\'{}\') and date < date(\'{}\') and dayofweek(date) = {} and rate <0'
                         .format(self.table, self.start, self.end, i))
            sum_dec, counter_dec = self.cursor.fetchall()[0]
            print '{:8.4f}'.format(counter_inc /float(counter_inc+counter_dec)), '{:8.4f}'.format((sum_inc+sum_dec)/(counter_dec+counter_inc))

    def rate_st_month(self):
        for i in range(1, 13):
            self.execute('select sum(rate), count(*) from sh  where date > date(\'{}\') and date < date(\'{}\') and month(date) = {}'
                         .format(self.start, self.end, i))
            sum_rate, count = self.cursor.fetchall()[0]
            print '{:8.4f}'.format(sum_rate/count)







if __name__ == '__main__':
    db = Dbstatistic()
    #db.initialize('b000001')
    #print  db._get_start_date(10)
    #print db.fluctuation_in_recent_days(5)
    flu_dict = {}
    for code in code_list.keys() + market_index_list.keys():
        db.initialize(code)
        code_flu = db.fluctuation_in_recent_days(1)
        flu_dict[code] = code_flu

    print sorted(flu_dict.items(), key=lambda d: float(d[1]))[-10:]
    #print flu_dict


    #sh.rate_st_weekday()
    #sh.rate_st_month()
    #print sh.get_start_date






    #for i in range(2, 7):
#    cur.execute('select count(*) from sh where dayofweek(date) = {} and rate >0'.format(i))
#    counter_inc = cur.fetchall()[0][0]
#    cur.execute('select count(*) from sh where dayofweek(date) = {} and rate <0'.format(i))
#    counter_dec = cur.fetchall()[0][0]
#    print counter_inc /float(counter_inc+counter_dec)


#for i in range(2, 7):
#    cur.execute('select count(rate),sum(rate) from sh where dayofweek(date) = {} and rate >0'.format(i))
#    res = cur.fetchall()[0]
#    print res[1] / res[0]