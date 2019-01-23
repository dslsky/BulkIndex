# -*- coding:utf-8 -*-

from HbaseTools import HbaseInfoTask
from RedisTools import RedisTools
from elasticsearch import Elasticsearch
import logging
from conf import ES_ADDR
from util import trans_md5

logging.basicConfig(filename='log/xw_del_info.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s :%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S %p', level=logging.INFO)

class NewsDelInfo(object):

    def __init__(self):
        self.hbase_con = HbaseInfoTask()
        self.redis_con = RedisTools()
        self.es = Elasticsearch(ES_ADDR, ignore=404)

    def es_ping(self):
        if not self.es.ping():
            self.es = Elasticsearch(ES_ADDR, ignore=404)

    def run(self):
        while True:
            rowkey = self.redis_con.get_yy_rowkey("es:news:del:info")
            _id = trans_md5(rowkey)
            self.es_ping()
            try:
                boo = self.es.exists(index="xw_info",doc_type="sino",id=_id)
                if boo:
                    self.es.delete(index="xw_info",doc_type="sino",id=_id)
            except Exception as e:
                log_info = "news info delete error %s" %str(e)
                logging.error(log_info)
                boo = self.es.exists(index="xw_info", doc_type="sino", id=_id)
                if boo:
                    self.es.delete(index="xw_info", doc_type="sino", id=_id)

if __name__=="__main__":
    newsDelInfo = NewsDelInfo()
    newsDelInfo.run()