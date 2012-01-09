#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import simplejson as sj
import redis
from pymmseg import mmseg
from config import Config
from exception import (
    ParameterError,
)

class Search(object):
    '''搜索类'''

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

    def query(self, field, keyword, count):
        '''关键词搜索'''

        results = []
        if keyword is None or keyword == '':
            return results

        # 转成UTF-8
        try:
            keyword = keyword.encode('utf-8')
        except UnicodeDecodeError, e:
            pass

        max_length = max(50, count)
        keyword = keyword.lower()

        # 分词
        algor = mmseg.Algorithm(keyword)
        for tok in algor:
            # 不区分大小写
            word = tok.text.decode('utf-8').lower()
            results.append(word)

        # 所有关键词结果的交集的key
        temp_store_key = 'tmpinter:%s:%s' % (field, '+'.join(results))
        # 关键词结果的并集的缓存不存在
        if self.r.exists(temp_store_key) is False:
            # 关键词结果的交集缓存
            cnt = self.r.sinterstore(temp_store_key, ['%s:%s' % (field, result) for result in results])
            if cnt == 0:
                return []
            # 缓存时间
            self.r.expire(temp_store_key, 60 * 5)

        # 如果建立了相关索引，可以通过sort直接获取数据
        # return self.r.sort(temp_store_key, by='%s:score:%s' % (field, '*'), get='%s:%s' % (field, '*'), start=0, num=count, desc=True)

        # 获取id
        ids = self.r.sort(temp_store_key, by='%s:score:%s' % (field, '*'), start=0, num=count, desc=True)
        # 获取数据
        return self.r.hmget(field, ids)

    def complete(self, field, prefix, count):
        '''前缀索引搜索'''

        results = []
        if prefix is None or prefix == '':
            return results

        # 转成UTF-8
        try:
            prefix = prefix.encode('utf-8')
        except UnicodeDecodeError, e:
            pass

        rangelen = 20
        max_length = max(50, count)
        prefix = prefix.lower()
        # 找到起始位置
        start = self.r.zrank('compl:%s' % (field), prefix)
        if start is None:
            return results
        # 不满足结果个数
        while len(results) != max_length:
            # 默认取出rangelen个，小于MTU
            ranges = self.r.zrange('compl:%s' % (field), start, start + rangelen - 1)
            # 累加起始位置
            start += rangelen
            # 没有结果了
            if ranges is None or len(ranges) == 0:
                break
            # 遍历结果
            for entry in ranges:
                # 比较输入和结果是否一样
                minlen = min(len(entry), len(prefix))
                # 如果不同
                if (entry[:minlen] != prefix[:minlen]):
                    max_length = len(results)
                    break
                # 如果是原始字符并且结果不等于结果个数
                if entry[-1] == '*' and len(results) != max_length:
                    results.append(entry[0:-1])

        # 所有前缀结果的并集的key
        temp_store_key = 'tmpunion:%s:%s' % (field, '+'.join(results))
        # 前缀结果的并集的缓存不存在
        if self.r.exists(temp_store_key) is False:
            # 前缀结果的并集缓存
            self.r.sunionstore(temp_store_key, ['%s:%s' % (field, result) for result in results])
            # 缓存时间
            self.r.expire(temp_store_key, 60 * 5)

        # 如果建立了相关索引，可以通过sort直接获取数据
        # return self.r.sort(temp_store_key, by='%s:score:%s' % (field, '*'), get='%s:%s' % (field, '*'), start=0, num=count, desc=True)

        # 获取id
        ids = self.r.sort(temp_store_key, by='%s:score:%s' % (field, '*'), start=0, num=count, desc=True)
        # 获取数据
        return self.r.hmget(field, ids)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'error'
        sys.exit()
    redis_config = {'host' : 'localhost', 'port' : 6379, 'db' : 0}
    kwargs = {'redis' : redis_config}
    kwargs = {'config' : Config(**kwargs)}
    search = Search(**kwargs)
    word = sys.argv[1].decode('utf-8')
    results = search.complete('title', word, 20)
    for result in results:
        print result
    print '====================='
    results = search.query('title', word, 20)
    for result in results:
        print result
