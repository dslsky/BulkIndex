# -*- coding:utf-8 -*-

from HbaseTools import HbaseInfoTask
from RedisTools import RedisTools
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
import logging
from conf import ES_SF_ADDR,COUNT_NUM

logging.basicConfig(filename='log/audioaas.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s :%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S %p', level=logging.INFO)

class Audioass(object):

    def __init__(self):
        self.hbase_con = HbaseInfoTask()
        self.redis_con = RedisTools()
        self.es = Elasticsearch(ES_SF_ADDR)

    def es_ping(self):
        if not self.es.ping():
            self.es = Elasticsearch(ES_SF_ADDR)

    def run(self):
        action_list = []
        count = 0
        start = int(time.time())
        while True:
            rowkey = self.redis_con.get_rowkey("audioaas")
            if rowkey == None:
                if len(action_list) > 0:
                    self.commit(action_list)
                    action_list.clear()
                    count = 0
                    start = int(time.time())
                time.sleep(10)
                continue
            if "|||||" in rowkey:
                rowkey = rowkey.split("|||||")[0]
            map = self.hbase_con.getSuanfaResultByRowkey("AUDIO_AAS_MAP_TABLE", rowkey, "audioaas")
            if not map:
                continue
            action = {
                "_index": "audioaas",
                "_type": "sino",
                "_id": "",
                "_source": {},
            }
            action['_id'] = rowkey
            action['_source'] = map
            action_list.append(action)
            end = int(time.time())
            count = count + 1
            if count > COUNT_NUM or (end-start) > 10:
                if len(action_list) > 0:
                    self.es_ping()
                    self.commit(action_list)
                    start = int(time.time())
                    action_list.clear()
                count = 0

    def commit(self,action_list):
        try:
            helpers.bulk(self.es, action_list)
        except Exception as e:
            log_info = "index:audioass,\terror:" + str(e)
            logging.error(log_info)
            helpers.bulk(self.es, action_list)
        logging.info("提交成功")

if __name__=="__main__":
    audioass = Audioass()
    audioass.run()

