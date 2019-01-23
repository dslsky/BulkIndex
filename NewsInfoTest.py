# -*- coding:utf-8 -*-

from HbaseTools import HbaseInfoTask
from RedisTools import RedisTools
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
import logging
from conf import ES_ADDR,COUNT_NUM
from util import trans_md5

logging.basicConfig(filename='log/xw_info_test.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s :%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S %p', level=logging.WARNING)

class GetInfo(object):

    def __init__(self):
        self.hbase_con = HbaseInfoTask()
        self.redis_con = RedisTools()
        self.es = Elasticsearch(ES_ADDR,timeout=30)

    def es_ping(self):
        if not self.es.ping():
            self.es = Elasticsearch(ES_ADDR,timeout=30)

    def run(self):
        action_list = []
        count = 0
        start = int(time.time())
        cunzai = 0
        while True:
            rowkey = self.redis_con.get_rowkey("xw_info_test")
            if rowkey == None:
                if len(action_list) > 0:
                    logging.warning("重复存入elasticsearch当中%d条数据" % cunzai)
                    cunzai = 0
                    self.commit(action_list)
                    action_list.clear()
                    start = int(time.time())
                    count = 0
                time.sleep(10)
                continue
            else:
                self.redis_con.redis_con.hset("es:news:info:tongji", rowkey.split("|||||")[0], 1)
            param = None
            if "|||||" in rowkey:
                params = rowkey.split("|||||")[1]
                param = params.split(",")
                rowkey = rowkey.split("|||||")[0]
            _id = trans_md5(rowkey)
            boo = self.es.exists("xw_info", "sino", _id)
            action = {
                "_index": "xw_info",
                "_type": "sino",
                "_id": "",
            }
            if boo:
                map = self.hbase_con.getResultByRowkey("INFO_TABLE", rowkey, "xw_info",param)
                if not map:
                    continue
                action["_op_type"] = "update"
                action['doc'] = map
                cunzai = cunzai+1
                print("修改"+rowkey)
            else:
                map = self.hbase_con.getResultByRowkey("INFO_TABLE", rowkey, "xw_info")
                if not map:
                    continue
                action['_source'] = map
                print("新增" + rowkey)
                self.redis_con.set_rowkey("xw_info_test",rowkey)
            if map['content_md5']:
                self.redis_con.redis_con.hset("es:news:info:tongji", rowkey, map['content_md5'])
            action['_id'] = _id
            action_list.append(action)
            end = int(time.time())
            count = count + 1
            if count > COUNT_NUM or (end - start) > 20:
                self.es_ping()
                logging.warning("重复存入elasticsearch当中%d条数据" % cunzai)
                cunzai = 0
                start = int(time.time())
                self.commit(action_list)
                action_list.clear()
                count = 0

    def commit(self,action_list):
        try:
            helpers.bulk(self.es, action_list)
        except Exception as e:
            log_info = "index:xw_info index,\terror:" + str(e)
            logging.error(log_info)
            print(log_info)
            helpers.bulk(self.es, action_list)
        logging.warning("提交成功：%d条数据" % len(action_list))
        print("提交成功：%d条数据" % len(action_list))

if __name__=="__main__":
    getInfo = GetInfo()
    getInfo.run()

