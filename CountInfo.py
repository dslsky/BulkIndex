# -*- coding:utf-8 -*-

from HbaseTools import HbaseInfoTask
from RedisTools import RedisTools
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
import logging
from conf import ES_ADDR,COUNT_NUM,ELASTIC_TIMEOUT
from util import trans_md5

logging.basicConfig(filename='log/forum_info.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s :%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S %p', level=logging.WARNING)

class GetCountInfo(object):

    def __init__(self):
        self.hbase_con = HbaseInfoTask()
        self.redis_con = RedisTools()
        self.es = Elasticsearch(ES_ADDR,timeout=ELASTIC_TIMEOUT)

    def es_ping(self):
        if not self.es.ping():
            self.es = Elasticsearch(ES_ADDR,timeout=ELASTIC_TIMEOUT)

    def run(self):
        action_list = []
        count = 0
        start = int(time.time())
        cunzai = 0
        while True:
            rowkey = self.redis_con.get_rowkey("statistics_info")
            # logging.info(rowkey)
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
            param = None
            if "|||||" in rowkey:
                params = rowkey.split("|||||")[1]
                param = params.split(",")
                rowkey = rowkey.split("|||||")[0]
            #这个地方是将hbase的rowkey转化为md5当做es的id，但是只有wx_info，xw_info，forum_info需要这么做
            # _id = trans_md5(rowkey)
            boo = self.es.exists("statistics_info","sino",rowkey)
            if boo:
                #更新数据
                cunzai = cunzai + 1
                map = self.hbase_con.getResultByRowkey("COUNT_INFO_TABLE", rowkey, "statistics_info",param)
                if not map:
                    continue
                action_list.append({
                    "_op_type":"update",
                    "_index": "statistics_info",
                    "_type": "sino",
                    "_id": rowkey,
                    "doc": map,
                })
            else:
                map = self.hbase_con.getResultByRowkey("COUNT_INFO_TABLE", rowkey, "statistics_info")
                if not map:
                    continue
                #新增数据
                action_list.append({
                    "_index": "statistics_info",
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
            log_info = "index:statistics_info,\terror:" + str(e)
            logging.error(log_info)
            helpers.bulk(self.es, action_list)
        logging.warning("提交成功：%d条数据" % len(action_list))

if __name__=="__main__":
    getCountInfo = GetCountInfo()
    #getCountInfo是随便定义的变量，不过一般class名字的首字母是大写的，然后这个变量把它首字母写成小写的，就可以
    getCountInfo.run()

