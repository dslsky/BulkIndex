# -*- coding:utf-8 -*-

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from hbase import THBaseService
from hbase.ttypes import *
import logging
from conf import *

class HbaseInfoTask(object):

    def __init__(self):
        transport = TSocket.TSocket(HBASE_ADDERSS, HBASE_PORT)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        transport.open()
        self.client = THBaseService.Client(protocol)

    def get_client(self):
        transport = TSocket.TSocket(HBASE_ADDERSS, HBASE_PORT)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        transport.open()
        self.client = THBaseService.Client(protocol)

    '''
     * create by: yangjt
     * description:采集hbase表数据获取并处理
     * create time:  
     * 
         table：hbase表名（采集表）
         rowkey：hbase单条数据的rowkey
         type：限制放入elasticsearch中的hbase数据字段
         parmas：限制放入elasticsearch中的hbase数据字段
     * @return 
    '''
    def getResultByRowkey(self,table,rowkey,type,params=None):
        values = {}
        get = TGet()
        get.row = rowkey.encode()
        tableName = table.encode()
        try:
            result = self.client.get(tableName,get)
            # 遍历hbase单条数据
            for column in result.columnValues:
                value = column.value.decode(errors="ignore")
                if str(value).strip() == "":
                    value = None
                # 限制放入elasticsearch中的hbase字段
                if column.qualifier.decode() in ziduan[type].keys():
                    #在params中的字段才可以存入elasticsearch
                    if params != None:
                        if column.qualifier.decode() in params:
                            #hbase字段名到elasticsearch字段名转化
                            values[ziduan[type][column.qualifier.decode()]]=value
                    #存入全部的elasticsearch业务字段
                    else:
                        values[ziduan[type][column.qualifier.decode()]] = value
            if type == "wx_info" or type == "wechat" or type == "wx_group":
                if "publish_time" in values.keys():
                    if values['publish_time'] != None:
                        values['publish_time'] = values['publish_time'] + " 00:00:00"
            if not values:
                log_info = "rowkey为%s的值为%s" %(rowkey,str(values))
                logging.error(log_info)
            if "thematic_image" in values.keys():
                thematic_image = values['thematic_image']
                if values['thematic_image'] != None and len(thematic_image) > 30000:
                    values['thematic_image'] = ""
                    log_info = "rowkey为%s的文章的专题图片字段长度超过30000" % rowkey
                    logging.warning(log_info)
            return values
        #报错处理机制，一般是由于连接断开引起，代码同上
        except Exception as e:
            logging.error(str(e))
            self.get_client()
            result = self.client.get(tableName, get)
            for column in result.columnValues:
                value = column.value.decode(errors="ignore")
                if str(value).strip() == "":
                    value = None
                if column.qualifier.decode() in ziduan[type].keys():
                    if params != None:
                        if column.qualifier.decode() in params:
                            values[ziduan[type][column.qualifier.decode()]] = value
                    else:
                        values[ziduan[type][column.qualifier.decode()]] = value
            if type == "wx_info" or type == "wechat" or type == "wx_group":
                if "publish_time" in values.keys():
                    if values['publish_time'] != None:
                        values['publish_time'] = values['publish_time'] + " 00:00:00"
            if not values:
                log_info = "rowkey为%s的值为%s" % (rowkey, str(values))
                logging.error(log_info)
            if "thematic_image" in values.keys():
                thematic_image = values['thematic_image']
                if values['thematic_image'] != None and len(thematic_image) > 30000:
                    values['thematic_image'] = ""
                    log_info = "rowkey为%s的文章的专题图片字段长度超过30000" % rowkey
                    logging.warning(log_info)
            return values

    '''
     * create by: yangjt
     * description:算法hbase表数据获取并处理
     * create time:  
     * 
         table：hbase表名（算法表）
         rowkey：hbase单条数据的rowkey
         type：限制放入elasticsearch中的hbase数据字段
         parmas：限制放入elasticsearch中的hbase数据字段
     * @return 
    '''
    def getSuanfaResultByRowkey(self,table,rowkey,type,params=None):
        values = {}
        get = TGet()
        get.row = rowkey.encode()
        tableName = table.encode()
        try:
            result = self.client.get(tableName,get)
            #遍历hbase单条数据
            for column in result.columnValues:
                value = column.value.decode(errors="ignore")
                if str(value).strip() == "":
                    value = None
                #限制放入elasticsearch中的hbase字段
                if column.qualifier.decode() in list_ziduan[type]:
                    if params != None:
                        if column.qualifier.decode() in params:
                            values[column.qualifier.decode()] = value
                    else:
                        values[column.qualifier.decode()] = value
            #特殊字段值处理
            if type == "audioaas":
                if "pubtime" in values.keys():
                    values['publish_time'] = values['pubtime']
                    values.pop("pubtime")
                if "spidtime" in values.keys():
                    values['spider_time'] = values['spidtime']
                    values.pop("spidtime")
            return values
        #报错处理机制，同上
        except Exception as e:
            logging.error(str(e))
            self.get_client()
            result = self.client.get(tableName, get)
            for column in result.columnValues:
                value = column.value.decode(errors="ignore")
                if str(value).strip() == "":
                    value = None
                if params != None:
                    if column.qualifier.decode() in params:
                        values[column.qualifier.decode()] = value
                else:
                    values[column.qualifier.decode()] = value
            if type == "audioaas":
                if "pubtime" in values.keys():
                    values['publish_time'] = values['pubtime']
                    values.pop("pubtime")
                if "spidtime" in values.keys():
                    values['spider_time'] = values['spidtime']
                    values.pop("spidtime")
            return values

    '''
     * create by: yangjt
     * description:应用hbase表数据获取并处理
     * create time:  
     * table：应用hbase表名
        rowkey：应用hbase表单条表名
     * @return 
    '''
    def getYyResultByRowkey(self,table,rowkey):
        values = {}
        get = TGet()
        get.row = rowkey.encode()
        tableName = table.encode()
        try:
            result = self.client.get(tableName,get)
            for column in result.columnValues:
                value = column.value.decode(errors="ignore")
                if str(value).strip() == "":
                    value = None
                values[column.qualifier.decode()] = value
            if "id" in values.keys():
                values.pop("id")
            return values
        except Exception as e:
            logging.error(str(e))
            self.get_client()
            result = self.client.get(tableName, get)
            for column in result.columnValues:
                value = column.value.decode(errors="ignore")
                if str(value).strip() == "":
                    value = None
                values[column.qualifier.decode()] = value
            if "id" in values.keys():
                values.pop("id")
            return values

class HbaseLogInfoTask(object):

    def __init__(self):
        transport = TSocket.TSocket(HBASE_LOG_ADDERSS, HBASE_LOG_PORT)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        transport.open()
        self.client = THBaseService.Client(protocol)

    def get_client(self):
        transport = TSocket.TSocket(HBASE_LOG_ADDERSS, HBASE_LOG_PORT)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        transport.open()
        self.client = THBaseService.Client(protocol)

    def getResultByRowkey(self,table,rowkey):
        values = {}
        get = TGet()
        get.row = rowkey.encode()
        tableName = table.encode()
        try:
            result = self.client.get(tableName,get)
            for column in result.columnValues:
                value = column.value.decode(errors="ignore")
                if str(value).strip() == "":
                    value = None
                values[column.qualifier.decode()] = value
            return values
        except Exception as e:
            logging.error(str(e))
            self.get_client()
            result = self.client.get(tableName, get)
            for column in result.columnValues:
                value = column.value.decode(errors="ignore")
                if str(value).strip() == "":
                    value = None
                values[column.qualifier.decode()] = value
            return values