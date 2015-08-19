#!/root/anaconda/bin/python
import MySQLdb


conn = MySQLdb.connect(host='localhost', user='root', passwd='autott', db='test')
cur = conn.cursor()



for i in range(2,7):
    cur.execute('select count(*) from sh where dayofweek(date) = {} and rate >0'.format(i))
    counter_inc = cur.fetchall()[0][0]
    cur.execute('select count(*) from sh where dayofweek(date) = {} and rate <0'.format(i))
    counter_dec = cur.fetchall()[0][0]
    print counter_inc /float(counter_inc+counter_dec)


for i in range(2,7):
    cur.execute('select count(rate),sum(rate) from sh where dayofweek(date) = {} and rate >0'.format(i))
    res = cur.fetchall()[0]
    print res[1] / res[0]
