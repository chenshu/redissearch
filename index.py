#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import with_statement
import sys
import simplejson as sj
import redis
from pymmseg import mmseg
from config import Config
from exception import (
    ParameterError,
)

class Index(object):
    '''索引类'''

    def __init__(self, *args, **kwargs):
        # 参数检查
        if args:
            if len(args) % 2 != 0:
                raise ParameterError("Config requires an equal number of values and scores")
        # 动态初始化实例变量
        for i in range(len(args) / 2):
            setattr(self, args[i * 2], args[i * 2 + 1])
        for key in kwargs:
            setattr(self, key, kwargs[key])
        # redis
        pool = redis.ConnectionPool(host=self.config.redis['host'], port=self.config.redis['port'], db=self.config.redis['db'])
        self.r = redis.Redis(connection_pool=pool)
        #self.r = redis.StrictRedis(host=self.config.redis['host'], port=self.config.redis['port'], db=self.config.redis['db'])
        self.pipeline = self.r.pipeline()
        # 加载分词
        mmseg.dict_load_defaults()

    def add(self, tid, title, field, score = 0):
        '''建立索引'''

        # 数据检查
        if tid is None or tid == '' \
                or title is None or title == '' \
                or field is None or field == '':
            return False

        # 原始数据
        self.tid = tid
        self.title = title
        self.field = field
        self.data = {'tid' : self.tid, 'title' : self.title, 'field' : self.field}
        self.score = score

        # 原始数据保存到Hash
        #self.r.hset(self.field, self.tid, sj.dumps(self.data))
        self.pipeline.hset(self.field, self.tid, sj.dumps(self.data))
        # 采用下面的方法可以直接通过sort命令的get参数获取数据
        # self.pipeline.set('%s:%s' % (self.field, self.tid), sj.dumps(self.data))

        # 分词
        algor = mmseg.Algorithm(self.title)

        words = []
        for tok in algor:
            # 不区分大小写
            word = tok.text.decode('utf-8').lower()
            words.append(word)
            # 建立倒排索引，以分词为key
            #self.r.sadd('%s:%s' % (self.field, word), self.tid)
            self.pipeline.sadd('%s:%s' % (self.field, word), self.tid)
        # 用于搜索结果排序的score
        #self.r.set('%s:score:%s' % (self.field, self.tid), self.score)
        self.pipeline.set('%s:score:%s' % (self.field, self.tid), self.score)

        # 前缀索引
        if self.config.prefix_index_enable is True:
            # 不区分大小写
            word = self.title.decode('utf-8').lower()
            # 前缀索引不包括分词内容
            del words[:]
            words.append(word)
            # 建立倒排索引，以分词为key
            #self.r.sadd('%s:%s' % (self.field, word), self.tid)
            self.pipeline.sadd('%s:%s' % (self.field, word), self.tid)

            dic = []
            for word in words:
                for i in range(len(word)):
                    prefix = word[:i+1]
                    dic.append(prefix)
                    dic.append(0.0)
                    #print prefix.encode('utf-8')
                # 完整的词增加一项，用*号区分
                prefix = '%s%s' % (word, '*')
                dic.append(prefix)
                dic.append(0.0)
            #self.r.zadd('compl:%s' % (self.field), *dic)
            self.pipeline.zadd('compl:%s' % (self.field), *dic)

        if self.config.batch_create_enable == False:
            self.pipeline.execute()
        return True

    def create(self):
        if self.config.batch_create_enable == True:
            self.pipeline.execute()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'error'
        sys.exit()
    redis_config = {'host' : 'localhost', 'port' : 6379, 'db' : 0}
    kwargs = {'redis' : redis_config, 'prefix_index_enable' : True, 'batch_create_enable' : False}
    kwargs = {'config' : Config(**kwargs)}
    index = Index(**kwargs)
    #index.add('c088888888', '西子小小', 'title')
    #index.add('c055555555', 'Awk教程', 'title')
    #index.add('c066666666', '失恋33天电影', 'title')
    #index.add('c000027a1s', 'Cisco.Press.Cisco.Self-Study.Implementing.IPv6.Networks.pdf', 'title')
    with open(sys.argv[1]) as fp:
        i = 0
        for line in fp:
            i = i + 1
            if i % 10000 == 0:
                print i
            tid, uid, title, attachments = line.strip().split('\t')
            index.add(tid, title, 'title')
