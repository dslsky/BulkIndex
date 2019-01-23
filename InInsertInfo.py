# -*- coding:utf-8 -*-

from HbaseTools import HbaseInfoTask
from RedisTools import RedisYyTools
from elasticsearch import Elasticsearch
import logging
from conf import ES_ADDR

logging.basicConfig(filename='log/in_follow.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s :%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S %p', level=logging.INFO)

class YyInInfo(object):

    def __init__(self):
        self.hbase_con = HbaseInfoTask()
        self.redis_con = RedisYyTools()
        self.es = Elasticsearch(ES_ADDR)

    def es_ping(self):
        if not self.es.ping():
            self.es = Elasticsearch(ES_ADDR)

    def run(self):
        while True:
            rowkey = self.redis_con.get_yy_rowkey("es:in:insert:info")
            map = self.hbase_con.getYyResultByRowkey("IN_INFO_TABLE", rowkey)
            self.es_ping()
            self.es.index("in_follow",doc_type="sino",id=rowkey,body=map)

if __name__=="__main__":
    yyInInfo = YyInInfo()
    yyInInfo.run()

