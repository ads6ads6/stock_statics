from datetime import datetime
import sys
sys.path.append("../")
from dbbase import Dbbase
from map_code import code_list, market_index_list


class Evaluation(Dbbase):
    def __init__(self):
        super(Evaluation, self).__init__()
        self.exist = None
        self.code = None

    def initialize(self, code):
        self.code = code
        if not self.code.startswith('b'):
            self.code = "`{}`".format(code)

    def get_info(self, attribute, start='2000-01-01', end='2015-11-20'):;
        self.execute("select {} from {} where date > '{}' and date< '{}'".format(attribute, self.code, start, end))
        return [item[0] for item in self.cursor.fetchall()]


def uplimit_to_raise(code, start='2000-01-01', end='2015-11-29'):
    el = Evaluation()
    el.initialize(code)
    info = el.get_info(start=start, end=end, attribute='change_rate')
    info_open = el.get_info(start=start, end=end, attribute='open')
    leng_info = len(info)
    above = 0
    low = 0
    above_rate = []
    low_rate = []
    third_day = []
    fourth_day = []
    five_day = []
    for i in xrange(leng_info):
        if i > leng_info - 4:
            continue
        if -11 < info[i] < -5:
            #if -1 < info[i+1] < 1 :
                #if -10 < info[i+2] < -1:
                    low_rate.append(info[i+1])
                    third_day.append(info[i+2])
                    fourth_day.append(info[i+3])
                #five_day.append(info[i+4])
    print leng_info, zip(low_rate, third_day, fourth_day),sum(low_rate)/len(low_rate), \
        sum(third_day)/len(third_day),\
        sum(fourth_day)/len(fourth_day), #sum(five_day)/len, len([rate for rate in low_rate if rate > 0])/float(len(low_rate))


def l2h(code, start='2000-01-01', end='2015-11-29'):
    pass


if __name__ == '__main__':
    uplimit_to_raise('002142')





