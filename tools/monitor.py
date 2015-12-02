import  tushare as ts
from map_code import code_list
from time import sleep

monitor_code = ['sh', 'cyb', '000938', '000977', '002024', '600109', '601766', '600590' ]



while True:
    for code in monitor_code:
        if code in ['sh', 'cyb']:
            print '', ts.get_realtime_quotes(code).T.values[3]
        else:
            print code_list[code], ts.get_realtime_quotes(code).T.values[3]
    sleep(10)

