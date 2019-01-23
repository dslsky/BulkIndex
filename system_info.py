# -*- coding:utf-8 -*-

import time
import logging
from HbaseTools import HbaseLogInfoTask
from RedisTools import RedisYyTools
from elasticsearch import Elasticsearch,helpers
from conf import ES_LOG_ADDR,COUNT_NUM

logging.basicConfig(filename='log/log_info.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s :%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S %p', level=logging.INFO)

class SystemInsertInfo(object):

    def __init__(self):
        self.hbase_con = HbaseLogInfoTask()
        self.redis_con = RedisYyTools()
        self.es = Elasticsearch(ES_LOG_ADDR)

    def es_ping(self):
        if not self.es.ping():
            self.es = Elasticsearch(ES_LOG_ADDR,timeout=30)

    def run(self):
        action_list = []
        count = 0
        start = int(time.time())
        cunzai = 0
        while True:
            rowkey = self.redis_con.get_yy_nb_rowkey("es:system:insert:info")
            if rowkey == None:
                if len(action_list) > 0:
                    logging.warning("重复存入elasticsearch当中%d条数据" % cunzai)
                    cunzai = 0
                    self.commit(action_list)
                    action_list.clear()
                    start = int(time.time())
                    count = 0
                time.sleep(5)
                continue
            if len(rowkey) > 500:
                log_info = "id：%s长度超过500" %rowkey
                logging.warning(log_info)
                continue
            boo = self.es.exists("system_info","sino",rowkey)
            if boo:
                cunzai = cunzai + 1
                map = self.hbase_con.getResultByRowkey("system_info", rowkey)
                if not map:
                    continue
                action_list.append({
                    "_op_type":"update",
                    "_index": "system_info",
                    "_type": "sino",
                    "_id": rowkey,
                    "doc": map,
                })
            else:
                map = self.hbase_con.getResultByRowkey("system_info", rowkey)
                if not map:
                    continue
                action_list.append({
                    "_index": "system_info",
                    "_type": "sino",
                    "_id": rowkey,
                    "_source": map,
                })
            end = int(time.time())
            count = count + 1
            if count > COUNT_NUM or (end - start) > 30:
                self.es_ping()
                logging.warning("重复存入elasticsearch当中%d条数据" % cunzai)
                cunzai = 0
                if len(action_list) > 0:
                    self.commit(action_list)
                start = int(time.time())
                action_list.clear()
                count = 0

    def commit(self,action_list):
        try:
            helpers.bulk(self.es, action_list)
        except Exception as e:
            log_info = "index:system_info,\terror:" + str(e)
            logging.error(log_info)
            helpers.bulk(self.es, action_list)
        logging.warning("提交成功：%d条数据" % len(action_list))

if __name__=="__main__":
    systemInsertInfo = SystemInsertInfo()
    systemInsertInfo.run()