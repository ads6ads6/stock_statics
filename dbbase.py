import MySQLdb
import sys
sys.path.append('../bin')
from dbinfo import dbinfo

class Dbbase(object):
    def __init__(self):
        self.conn = MySQLdb.connect(host=dbinfo['HOST_IP'], user=dbinfo['USER'], passwd=dbinfo['PASSWORD'], db='test')
        self.cursor = self.conn.cursor()
        self.execute = self.cursor.execute
