# -*- coding:utf-8 -*-

from HbaseTools import HbaseInfoTask
from RedisTools import RedisYyTools
from elasticsearch import Elasticsearch
import logging
from conf import ES_ADDR

logging.basicConfig(filename='log/ur_del_follow.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s :%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S %p', level=logging.WARNING)

class YyDelUrUser(object):

    def __init__(self):
        self.hbase_con = HbaseInfoTask()
        self.redis_con = RedisYyTools()
        self.es = Elasticsearch(ES_ADDR)

    def es_ping(self):
        if not self.es.ping():
            self.es = Elasticsearch(ES_ADDR)

    def run(self):
        while True:
            rowkey = self.redis_con.get_yy_rowkey("es:ur:del:info")
            logging.warning(rowkey)
            self.es_ping()
            try:
                boo = self.es.exists("ur_follow",doc_type="sino",id=rowkey)
                if boo:
                    self.es.delete("ur_follow",doc_type="sino",id=rowkey)
            except Exception as e:
                log_info = "rowkey : %s", str(e)
                logging.warning(log_info)

if __name__=="__main__":
    yyDelUrUser = YyDelUrUser()
    yyDelUrUser.run()

