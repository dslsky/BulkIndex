# -*- coding:utf-8 -*-

from HbaseTools import HbaseInfoTask
from RedisTools import RedisTools
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
import logging
from conf import ES_ADDR,COUNT_NUM
from util import trans_md5

logging.basicConfig(filename='log/wechat_info.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s :%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S %p', level=logging.WARNING)

class GetWechatInfo(object):

    '''
     * create by: yangjt
     * description:初始化hbase，redis-cluster，elasticsearch连接
     * create time:  
     * 
     
     * @return 
    '''
    def __init__(self):
        self.hbase_con = HbaseInfoTask()
        self.redis_con = RedisTools()
        self.es = Elasticsearch(ES_ADDR,timeout=30)

    def es_ping(self):
        if not self.es.ping():
            self.es = Elasticsearch(ES_ADDR,timeout=30)

    '''
     * create by: yangjt
     * description:WECHAT_INFO_TABLE数据同步
     * create time:  
     * 
     
     * @return 
    '''
    def run(self):
        action_list = []
        count = 0
        start = int(time.time())
        cunzai = 0
        while True:
            #获取需要同步的redis值
            rowkey = self.redis_con.get_rowkey("wx_info")
            #由于这里无法使用blpop，所以需要通过空值判定
            if rowkey == None:
                #没有进数据时，将累积的需要同步的数据存入elasticsearch
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
            #获取rowkey和需要同步的字段
            if "|||||" in rowkey:
                params = rowkey.split("|||||")[1]
                param = params.split(",")
                rowkey = rowkey.split("|||||")[0]
            #将hbase的rowkey转化为md5类型数据，存入elasticsearch
            _id = trans_md5(rowkey)
            #判定此_id是否存在于elasticsearch，花费时间为30毫秒，实为head请求
            boo = self.es.exists("wx_info", "sino", _id)
            action = {
                "_index": "wx_info",
                "_type": "sino",
                "_id": "",
            }
            #如果数据已存在，采取update的方式进行数据上传
            if boo:
                map = self.hbase_con.getResultByRowkey("WECHAT_INFO_TABLE", rowkey, "wx_info",param)
                if not map:
                    continue
                action["_op_type"] = "update"
                action['doc'] = map
                cunzai = cunzai + 1
            #如果数据不存在，采集insert的方式进行数据上传（此时不用去关心是否被限制了字段）
            else:
                map = self.hbase_con.getResultByRowkey("WECHAT_INFO_TABLE", rowkey, "wx_info")
                if not map:
                    continue
                action['_source'] = map
            action['_id'] = _id
            action_list.append(action)
            end = int(time.time())
            count = count + 1
            #如果数据量超过COUNT_NUM或者距离上次提交数据的时间超过30秒，则提交数据
            if count > COUNT_NUM or (end - start) > 30:
                self.es_ping()
                logging.warning("重复存入elasticsearch当中%d条数据" % cunzai)
                cunzai = 0
                self.commit(action_list)
                start = int(time.time())
                action_list.clear()
                count = 0

    '''
     * create by: yangjt
     * description:批量上传数据
     * create time:  
     * 
        action_list:{
                "_index": "wx_info",
                "_type": "sino",
                "_id": "",
                "_source":{"key":"value"}
            }
     * @return 
    '''
    def commit(self,action_list):
        try:
            helpers.bulk(self.es, action_list)
        except Exception as e:
            log_info = "index:wechat,\terror:" + str(e)
            logging.error(log_info)
            helpers.bulk(self.es, action_list)
        logging.warning("提交成功：%d条数据" % len(action_list))

#启动进程
if __name__=="__main__":
    getWechatInfo = GetWechatInfo()
    getWechatInfo.run()

