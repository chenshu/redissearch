#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import with_statement
import sys
from collections import defaultdict
import simplejson as sj
import redis
from pymmseg import mmseg
from config import Config
from exception import (
    ParameterError,
)

class BatchIndex(object):

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
        # self.r = redis.StrictRedis(host=self.config.redis['host'], port=self.config.redis['port'], db=self.config.redis['db'])
        self.pipeline = self.r.pipeline()
        # 加载分词
        mmseg.dict_load_defaults()

    def index(self, path, field):
        with open(path, 'r') as fp:
            i = 0
            data = {}
            inverted_index = defaultdict(list)
            scores = {}
            prefix_index = []
            for line in fp:
                i = i + 1
                if i % 20000 == 0:
                    print i
                    # 原始数据保存到Hash
                    self.r.hmset(field, data)
                    #self.pipeline.hmset(field, data)
                    # 建立倒排索引，以分词为key
                    for w in inverted_index:
                        self.pipeline.sadd('%s:%s' % (field, w), *inverted_index[w])
                    self.pipeline.execute()
                    # 用于搜索结果排序的score
                    self.r.mset(scores)
                    #self.pipeline.mset(scores)
                    # 前缀索引
                    self.r.zadd('compl:%s' % (field), *prefix_index)
                    #self.pipeline.zadd('compl:%s' % (field), *prefix_index)
                    #self.pipeline.execute()

                    data.clear()
                    inverted_index.clear()
                    scores.clear()
                    del prefix_index[:]

                tid, uid, title, attachments = line.strip().split('\t')
                score = 0
                data[tid] = sj.dumps({'tid' : tid, 'title' : title, 'field' : field})
                # 分词
                algor = mmseg.Algorithm(title)
                words = []
                for tok in algor:
                    # 不区分大小写
                    word = tok.text.decode('utf-8').lower()
                    words.append(word)
                    inverted_index[word].append(tid)
                scores['%s:score:%s' % (field, tid)] = score

                # 前缀索引
                if self.config.prefix_index_enable is True:
                    # 不区分大小写
                    word = title.decode('utf-8').lower()
                    # 前缀索引不包括分词内容
                    del words[:]
                    words.append(word)
                    inverted_index[word].append(tid)

                    for w in words:
                        for j in range(len(w)):
                            prefix = w[:j+1]
                            prefix_index.append(prefix)
                            prefix_index.append(0.0)
                        # 完整的词增加一项，用*号区分
                        prefix = '%s%s' % (w, '*')
                        prefix_index.append(prefix)
                        prefix_index.append(0.0)

            # 原始数据保存到Hash
            self.r.hmset(field, data)
            #self.pipeline.hmset(field, data)
            # 建立倒排索引，以分词为key
            for w in inverted_index:
                self.pipeline.sadd('%s:%s' % (field, w), *inverted_index[w])
            self.pipeline.execute()
            # 用于搜索结果排序的score
            self.r.mset(scores)
            #self.pipeline.mset(scores)
            # 前缀索引
            self.r.zadd('compl:%s' % (field), *prefix_index)
            #self.pipeline.zadd('compl:%s' % (field), *prefix_index)
            #self.pipeline.execute()

def main(path):
    redis_config = {'host' : 'localhost', 'port' : 6379, 'db' : 0}
    kwargs = {'redis' : redis_config, 'prefix_index_enable' : True}
    kwargs = {'config' : Config(**kwargs)}
    index = BatchIndex(**kwargs)
    index.index(path, 'title')

if __name__ == '__main__':
    if (len(sys.argv) != 2):
        print 'error'
        sys.exit()
    main(sys.argv[1])
