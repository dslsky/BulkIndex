# -*- coding:utf-8 -*-

from HbaseTools import HbaseInfoTask
from RedisTools import RedisTools
from elasticsearch import Elasticsearch,helpers
import logging
from conf import ES_SF_ADDR

logging.basicConfig(filename='log/image_del_info.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s :%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S %p', level=logging.WARNING)

class ImageDelInfo(object):

    def __init__(self):
        self.hbase_con = HbaseInfoTask()
        self.redis_con = RedisTools()
        self.es = Elasticsearch(ES_SF_ADDR, ignore=404)

    def es_ping(self):
        if not self.es.ping():
            self.es = Elasticsearch(ES_SF_ADDR, ignore=404)

    def delete_index(self,datas):
        action_list = []
        logging.warning("开始检查")
        for rowkey in datas:
            boo = self.es.exists(index="image", doc_type="sino", id=rowkey)
            if boo:
                action = {
                    "_op_type":"delete",
                    "_index": "image",
                    "_type": "sino",
                    "_id": rowkey,
                    "_source": {},
                }
                action_list.append(action)
        logging.warning("检查完毕：%d" %len(action_list))
        if len(action_list) > 0:
            self.commit(action_list)

    def commit(self,action_list):
        try:
            self.es_ping()
            helpers.bulk(self.es, action_list)
        except Exception as e:
            log_info = "index:image,\terror:" + str(e)
            logging.error(log_info)
            self.es_ping()
            helpers.bulk(self.es, action_list)

    def run(self):
        count = 0
        data_list = []
        while True:
            rowkey = self.redis_con.get_yy_rowkey("es:image:del:info")
            count = count + 1
            data_list.append(rowkey)
            if count > 3000:
                data_list = set(data_list)
                data_list = list(data_list)
                self.delete_index(data_list)
                data_list.clear()
                count = 0

if __name__=="__main__":
    imageDelInfo = ImageDelInfo()
    imageDelInfo.run()