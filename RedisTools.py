# -*- coding:utf-8 -*-

import redis
import rediscluster
from conf import *

class RedisTools(object):

    '''
     * create by: yangjt
     * description:初始化redis-cluster连接
     * create time:  
     * 
     
     * @return 
    '''
    def __init__(self):
        self.redis_con = rediscluster.StrictRedisCluster(REDIS_HOST,REDIS_PORT,decode_responses=True)

    def ping(self):
        if not self.redis_con.ping():
            self.redis_con = rediscluster.StrictRedisCluster(REDIS_HOST,REDIS_PORT,decode_responses=True)

    '''
     * create by: yangjt
     * description:获取redis当中list数据
     * create time:  
     * 
        type：redis的key值
     * @return 
    '''
    def get_rowkey(self,type):
        self.ping()
        result = self.redis_con.blpop(redis_insert_ziduan[type],1)
        if result == None:
            return  None
        else:
            key,value = result
            return value

    def set_rowkey(self, type,rowkey):
        self.ping()
        self.redis_con.rpush(redis_insert_ziduan[type], rowkey)

    def get_yy_rowkey(self,type):
        self.ping()
        key,result = self.redis_con.blpop(type)
        return result

    def insert_yy_rowkey(self,key,value):
        self.ping()
        self.redis_con.lpush(key,value)

class RedisYyTools(object):

    def __init__(self):
        self.redis_con = redis.StrictRedis(YY_HOST,YY_PORT,decode_responses=True)

    def ping(self):
        if not self.redis_con.ping():
            self.redis_con = redis.StrictRedis(YY_HOST,YY_PORT,decode_responses=True)

    def get_yy_rowkey(self,type):
        self.ping()
        key,result = self.redis_con.blpop(type)
        return result

    def get_yy_nb_rowkey(self,key):
        self.ping()
        result = self.redis_con.lpop(key)
        return result

class RedisYwTools(object):

    def __init__(self):
        self.redis_con = redis.StrictRedis(YW_HOST,YW_PORT,decode_responses=True)

    def ping(self):
        if not self.redis_con.ping():
            self.redis_con = redis.StrictRedis(YW_HOST,YW_PORT,decode_responses=True)

    def get_yy_rowkey(self,type):
        self.ping()
        key,result = self.redis_con.blpop(type)
        return result

    def get_yy_nb_rowkey(self,key):
        self.ping()
        result = self.redis_con.lpop(key)
        return result