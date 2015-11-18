from dbbase import Dbbase
from map_code import market_index_list, code_list
import random
import datetime


class ModelBase(Dbbase):
    def __init__(self, date='2010-01-01'):
        super(ModelBase, self).__init__()
        self.date = date
        self.result = {}
        self.top_code = []

    def check_trading_day(self):
        self.execute("select date from b000001 where date = '{}'".format(self.date))
        if self.cursor.fetchall():
            return True
        else:
            return False

    def check_last_working_day(self):
        self.execute("select date from b000001 where date < '{}' order by date desc limit 1".format(self.date))
        output = self.cursor.fetchall()
        return output[0][0]

    def get_price(self, title, code):
        self.execute("select {} from {} where date = '{}'".format(title, code, self.date))
        output = self.cursor.fetchall()
        return output[0][0]

    def get_result(self, title):
        for code in  ["`{}`".format(code) for code in code_list.keys()]:
            self.execute("select {} from {} where date = '{}'".format(title, code, self.date))
            output = self.cursor.fetchall()
            if not output:
                out = 0
            else:
                out = output[0][0]
            self.result[code] = out

    def get_pe(self, code):
        self.execute("select pe from stock_basic where code = '{}'".format(code))
        output = self.cursor.fetchall()
        if output[0][0] is None:
            return 10000
        else:
            return output[0][0]

    def filter_code(self):
        pass

    def get_one_code(self, from_last_working_day):
        pass



class ModelFluc(ModelBase):
    def __init__(self, date='2010-01-01'):
        super(ModelFluc, self).__init__()


    def filter_code(self):
        top_ten = sorted(self.result.items(), key=lambda x: x[1])[-10:]
        self.top_code = [item[0] for item in top_ten]
        #print [item for item in top_ten]

    def get_one_code(self, from_last_working_day=False):
        date = self.date
        if from_last_working_day:
            self.date = self.check_last_working_day()
        self.get_result('fluctuation')
        self.filter_code()
        base_rate = 0
        select = random.choice(self.top_code)
        for code in self.top_code:
            self.execute("select change_rate from {} where date = '{}'".format(code, self.date))
            output = self.cursor.fetchall()[0][0]
            if output > 9.9:
                continue
            if output > base_rate:
                select = code
                base_rate = output
        self.date = date
        return select


class ModelChangeRate(ModelBase):
    def __init__(self, date='2010-01-01'):
        super(ModelChangeRate, self).__init__()

    def filter_code(self):
        select_code = [code for code in self.result if 9 < self.result[code] < 9.9]
        num = len(select_code)
        if num < 30:
            select_code += [item[0] for item in sorted(self.result.items(), key=lambda x: x[1])[-30+num:]]
        return select_code


    def get_one_code(self, from_last_working_day=False):
        pe = {}
        date = self.date
        if from_last_working_day:
            self.date = self.check_last_working_day()
        self.get_result('change_rate')
        select_code = self.filter_code()
        for code in select_code:
            pe[code] = self.get_pe(code[1:-1])
        self.date = date
        top = sorted(pe.items(), key=lambda x: x[1])[:10]
        #print code, pe[code]
        return random.choice(top)[0]


class ModelReachHigh(ModelBase):
    def __init__(self):
        super(ModelReachHigh, self).__init__()

    def filter_code(self):
        select_code = []
        for code in ["`{}`".format(code) for code in code_list.keys()]:
            self.execute("select high from {} where date = '{}'".format(code, self.date))
            res = self.cursor.fetchall()
            if not res:
                continue
            high_today = res[0][0]
            self.execute("select count(date), max(high) from {} where date > '{}' and date < '{}'".
                         format(code, self.date-datetime.timedelta(365), self.date))
            output = self.cursor.fetchall()
            if not output:
                continue
            count, highest = output[0]
            #print count, highest, high_today, self.date-datetime.timedelta(365), self.date
            if count > 100 and high_today > highest:
                select_code.append(code)
        print select_code
        return select_code

    def get_one_code(self, from_last_working_day=False):
        date = self.date
        if from_last_working_day:
            self.date = self.check_last_working_day()
        selected = self.filter_code()
        self.date = date
        if not selected:
            return False
        return random.choice(selected)


def generate_output(model, start_date, end_date, interval):
    sold_open = False
    sold_close = False
    sold_fix = True
    buy_open = False
    buy_close = True
    buy_fix = False
    buy_from_last = False
    buy = False
    sold= False
    factor = 1
    buy_price = 1
    sold_price = 1
    code = ''
    curve = {}
    count = 0
    while start_date < end_date:
        model.date = start_date
        if not model.check_trading_day():
            start_date += datetime.timedelta(days=interval)
            continue
        if buy:
            if sold_open:
                try:
                    sold_price = model.get_price("open", code)
                except:
                    sold_price = buy_price
            elif sold_close:
                try:
                    sold_price = model.get_price("close", code)
                except:
                    sold_price = buy_price
            elif sold_fix:
                try:
                    highest = model.get_price("high", code)
                except:
                    highest = buy_price
                try:
                    lowest = model.get_price("low", code)
                except:
                    lowest = buy_price

                if highest/buy_price > 1.1:
                    sold_price = 1.1*buy_price
                    count = 0
                elif lowest/buy_price < 0.9:
                    sold_price = 0.9*buy_price
                    count = 0
                else:
                    count += 1
                    if count < 200:
                        start_date += datetime.timedelta(days=interval)
                        continue
                    else:
                        count = 0
                        try:
                            sold_price = model.get_price("close", code)
                        except:
                            sold_price = buy_price

            factor *= sold_price/buy_price
            sold = True
            buy = False
            curve[start_date] = factor
        if buy_from_last:
            code = model.get_one_code(from_last_working_day=True)
        else:
            code = model.get_one_code()

        if not code:
            start_date += datetime.timedelta(days=interval)
            continue

        if buy_open:
            try:
                buy_price = model.get_price('open', code)
            except:
                start_date += datetime.timedelta(days=interval)
                continue
        elif buy_close:
            try:
                buy_price = model.get_price('close', code)
            except:
                start_date += datetime.timedelta(days=interval)
                continue
        elif buy_fix:
            pass
        buy = True
        sold = False
        print start_date, factor, code, buy
        start_date += datetime.timedelta(days=interval)


if __name__ == '__main__':
    generate_output(model=ModelReachHigh(), start_date=datetime.date(2015, 1, 1),
                    end_date=datetime.date(2015, 10, 20), interval=1)











#def run():
#    test = ModelFluc()
#    code = "`600276`"
#    start_date = datetime.date(2015, 6, 15)
#    end_date = datetime.date(2015, 10, 20)
#    buy = False
#    sold = False
#    factor = 1
#    sold_price = 1
#    buy_price = 1
#    curve = {}
#    while start_date < end_date:
#        test.date = start_date
#        if not test.check_trading_day():
#            start_date += datetime.timedelta(days=1)
#            continue
#        if buy:
#            try:
#                sold_price = test.get_close_price(code)
#            except:
#                sold_price = buy_price
#            factor *= sold_price/buy_price
#            sold = True
#            buy = False
#            curve[start_date] = factor
#        elif test.get_change_rate(code) < -2.0:
#            buy_price = test.get_close_price(code)
#            buy = True
#            sold = False
#        start_date += datetime.timedelta(days=1)
#        print start_date, factor, code, buy






#if __name__ == '__main__':
#    run()
#    test = ModelFluc('2015-10-19')
#
#    start_date = datetime.date(2007, 1, 1)
#    end_date = datetime.date(2008, 10, 20)
#    factor = 1
#    curve = {}
#    sold_price = 1
#    buy_price = 1
#    buy = False
#    sold = False
#    code = ''
#    while start_date < end_date:
#        test.date = start_date
#        if not test.check_trading_day():
#            start_date += datetime.timedelta(days=1)
#            continue
#        if buy:
#            try:
#                sold_price = test.get_open_price(buy_code)
#            except:
#                sold_price = buy_price
#            factor *= sold_price/buy_price
#            sold = True
#            buy = False
#            curve[start_date] = factor
#        if code:
#            try:
#                buy_price = test.get_open_price(code)
#                buy_code = code
#            except:
#                pass
#        code = test.get_one_code()
#        buy = True
#        sold = False
#        start_date += datetime.timedelta(days=1)
#        print start_date, factor, code
#    print curve