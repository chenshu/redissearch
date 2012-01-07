#!/usr/bin/env python
# -*- coding: utf-8 -*-

from exception import (
    ParameterError,
)

class Config(object):
    '''搜索引擎的配置'''
    def __init__(self, *args, **kwargs):
        if args:
            if len(args) % 2 != 0:
                raise ParameterError("Config requires an equal number of values and scores")
        for i in range(len(args) / 2):
            setattr(self, args[i * 2], args[i * 2 + 1])
        for key in kwargs:
            setattr(self, key, kwargs[key])
        # 默认创建前缀索引
        if 'prefix_index_enable' not in kwargs:
            self.prefix_index_enable = True
        # 默认不批量创建索引
        if 'batch_create_enable' not in kwargs:
            self.batch_create_enable = False

if __name__ == '__main__':
    kwargs = {'host' : 'localhost', 'port' : 6379, 'db' : 0}
    c = Config('foo', 1, 'bar', 2, **kwargs)
    print hasattr(c, 'foo')
    print getattr(c, 'bar')
    print hasattr(c, 'host')
    print getattr(c, 'port')
    print c.db
