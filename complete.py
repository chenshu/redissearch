#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import with_statement
import sys
import redis
import simplejson as sj

def init(r):
    for fname in ['female-names.txt', 'chinese-names.txt']:
        with open(fname) as fp:
            kwargs = {}
            for line in fp:
                name = line.strip()
                name = name.decode('utf-8')
                for i in range(len(name)):
                    prefix = name[:i+1]
                    kwargs[prefix] = 0.0
                    #print i, prefix.encode('utf-8')
                prefix = '%s%s' % (name, '*')
                kwargs[prefix] = 0.0
            r.zadd('compl', **kwargs)

def complete(r, prefix, count):
    results = []
    rangelen = 10
    prefix = prefix.lower()
    # 找到起始位置
    start = r.zrank('compl', prefix)
    if start is None:
        return []
    # 不满足结果个数
    while len(results) != count:
        # 默认取出rangelen个，小于MTU
        ranges = r.zrange('compl', start, start + rangelen - 1)
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
                count = len(results)
                break
            # 如果是原始字符并且结果不等于结果个数
            if entry[-1] == '*' and len(results) != count:
                results.append(entry[0:-1])
    return results

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'error'
        sys.exit()
    pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
    r = redis.Redis(connection_pool=pool)
    #init(r)
    prefix = sys.argv[1]
    results = complete(r, prefix, 10)
    for result in results:
        print result
