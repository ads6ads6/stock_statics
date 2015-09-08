import MySQLdb

class Dbbase(object):
    def __init__(self):
        self.conn = MySQLdb.connect(host='120.26.211.94', user='root', passwd='autott', db='test')
        self.cursor = self.conn.cursor()
        self.execute = self.cursor.execute
