# -*- coding:utf-8 -*-

from HbaseTools import HbaseInfoTask
from RedisTools import RedisYyTools
from elasticsearch import Elasticsearch
import logging
from conf import ES_ADDR

logging.basicConfig(filename='log/ur_follow.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s :%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S %p', level=logging.INFO)

class YyUrUser(object):

    def __init__(self):
        self.hbase_con = HbaseInfoTask()
        self.redis_con = RedisYyTools()
        self.es = Elasticsearch(ES_ADDR)

    def es_ping(self):
        if not self.es.ping():
            self.es = Elasticsearch(ES_ADDR)

    def run(self):
        while True:
            rowkey = self.redis_con.get_yy_rowkey("es:ur:insert:info")
            map = self.hbase_con.getYyResultByRowkey("UR_USER_TABLE", rowkey)
            self.es_ping()
            self.es.index("ur_follow",doc_type="sino",id=rowkey,body=map)

if __name__=="__main__":
    yyUrUser = YyUrUser()
    yyUrUser.run()

