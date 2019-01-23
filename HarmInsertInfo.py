# -*- coding:utf-8 -*-

from HbaseTools import HbaseInfoTask
from RedisTools import RedisTools
from elasticsearch import Elasticsearch
import logging
from conf import ES_ADDR,HARM_INFO_ZIDUAN
from util import trans_md5

logging.basicConfig(filename='log/harm_info.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s :%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S %p', level=logging.INFO)

class HarmInsertInfo(object):

    '''
     * create by: yangjt
     * description:初始化hbase和redis连接
     * create time:  
     * 
     
     * @return 
    '''
    def __init__(self):
        self.hbase_con = HbaseInfoTask()
        self.redis_con = RedisTools()
        self.es = Elasticsearch(ES_ADDR)

    def es_ping(self):
        if not self.es.ping():
            self.es = Elasticsearch(ES_ADDR)

    '''
     * create by: yangjt
     * description:
     * create time:  
     * 
     
     * @return 
    '''
    def run(self):
        while True:
            result = self.redis_con.get_yy_rowkey("es:harm:insert:info")
            logging.info(result)
            rowkey,type = eval(result)
            _id = rowkey
            if type == "WECHAT_INFO_TABLE" or type == "INFO_TABLE" or type == "MONITOR_INFO_TABLE":
                _id = trans_md5(rowkey)
            log_info = "表格%s的rowkey的值为：%s" %(type,rowkey)
            logging.info(log_info)
            map = self.hbase_con.getResultByRowkey(type, rowkey,HARM_INFO_ZIDUAN[type])
            if not map:
                continue
            self.es_ping()
            boo = self.es.exists(HARM_INFO_ZIDUAN[type], "sino", _id)
            if boo:
                doc = {"doc": map}
                log_info = "rowkey值已存在"
                logging.info(log_info)
                self.es.update(HARM_INFO_ZIDUAN[type],doc_type="sino",id=_id,body=doc)
                log_info = "%s数据更新成功" %_id
                logging.info(log_info)
            else:
                log_info = "rowkey值:%s不存在" %_id
                logging.info(log_info)
                self.es.index(HARM_INFO_ZIDUAN[type],doc_type="sino",id=_id,body=map)

if __name__=="__main__":
    harmInsertInfo = HarmInsertInfo()
    harmInsertInfo.run()

