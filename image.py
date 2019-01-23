# -*- coding:utf-8 -*-

from HbaseTools import HbaseInfoTask
from RedisTools import RedisTools
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
import logging
from conf import ES_SF_ADDR,COUNT_NUM

logging.basicConfig(filename='log/image.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s :%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S %p', level=logging.WARNING)

class Image(object):

    def __init__(self):
        self.hbase_con = HbaseInfoTask()
        self.redis_con = RedisTools()
        self.es = Elasticsearch(ES_SF_ADDR)
        self.insert_count = 0

    def es_ping(self):
        if not self.es.ping():
            self.es = Elasticsearch(ES_SF_ADDR)

    def run(self):
        action_list = []
        count = 0
        start = int(time.time())
        cunzai = 0
        while True:
            rowkey = self.redis_con.get_rowkey("image")
            if rowkey == None:
                if len(action_list) > 0:
                    logging.warning("重复存入elasticsearch当中%d条数据" % cunzai)
                    cunzai = 0
                    self.commit(action_list)
                    action_list.clear()
                    count = 0
                    start = int(time.time())
                time.sleep(10)
                continue
            param = None
            if "|||||" in rowkey:
                params = rowkey.split("|||||")[1]
                param = params.split(",")
                rowkey = rowkey.split("|||||")[0]
            boo = self.es.exists("image", "sino", rowkey)
            action = {
                "_index": "image",
                "_type": "sino",
                "_id": "",
            }
            if boo:
                map = self.hbase_con.getSuanfaResultByRowkey("IMAGE_DATA_TABLE", rowkey, "image",param)
                if not map:
                    continue
                action["_op_type"] = "update"
                action['doc'] = map
                cunzai = cunzai+1
            else:
                map = self.hbase_con.getSuanfaResultByRowkey("IMAGE_DATA_TABLE", rowkey, "image")
                if not map:
                    continue
                try:
                    self.es.index(index="image", doc_type="sino", id=rowkey, body=map)
                    self.insert_count = self.insert_count + 1
                except Exception as e:
                    log_info = "单条插入错误：%s" % str(e)
                    logging.error(log_info)
                continue
                # action['_source'] = map
            action['_id'] = rowkey
            action_list.append(action)
            end = int(time.time())
            count = count + 1
            if count >COUNT_NUM or (end-start) > 30:
                logging.warning("重复存入elasticsearch当中%d条数据" %cunzai)
                cunzai = 0
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
            log_info = "index:image,\terror:" + str(e)
            logging.error(log_info)
            helpers.bulk(self.es, action_list)
        logging.warning("提交成功：%d条数据" %len(action_list))

if __name__=="__main__":
    image = Image()
    image.run()

