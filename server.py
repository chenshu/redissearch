#!/usr/bin/env python
#-*- coding: utf-8 -*-

import tornado.ioloop
import tornado.web
import tornado.httpclient
from tornado.escape import json_encode, json_decode
import time
from config import Config
from search import Search

class CompleteHandler(tornado.web.RequestHandler):

    def initialize(self, searcher):
        self.searcher = searcher

    @tornado.web.asynchronous
    def get(self):
        try:
            prefix = self.get_argument('q', None)
            self.t = self.get_argument('t', None)
            self.search('title', prefix, 10, self.async_callback(self.on_response))
        except UnicodeDecodeError, e:
            self.write(json_encode({'error' : 'cannot decode character'}))
            self.finsh()

    def search(self, field, prefix, count, callback):
        results = self.searcher.complete(field, prefix, count)
        response = []
        for result in results:
            response.append(json_decode(result))
        callback(response)

    def on_response(self, response):
        if (self.t is not None and self.t == 'jsonp'):
            self.write('%s%s' % ('r=', json_encode(response)))
        else:
            self.write(json_encode(response))
        self.finish()

class SearchHandler(tornado.web.RequestHandler):

    def initialize(self, searcher):
        self.searcher = searcher

    @tornado.web.asynchronous
    def get(self):
        try:
            keyword = self.get_argument('q', None)
            self.t = self.get_argument('t', None)
            self.search('title', keyword, 30, self.async_callback(self.on_response))
        except UnicodeDecodeError, e:
            self.write(json_encode({'error' : 'cannot decode character'}))
            self.finsh()

    def search(self, field, keyword, count, callback):
        results = self.searcher.query(field, keyword, count)
        response = []
        for result in results:
            response.append(json_decode(result))
        callback(response)

    def on_response(self, response):
        if (self.t is not None and self.t == 'jsonp'):
            self.write('%s%s' % ('r=', json_encode(response)))
        else:
            self.write(json_encode(response))
        self.finish()

redis_config = {'host' : 'localhost', 'port' : 6379, 'db' : 0}
kwargs = {'redis' : redis_config}
kwargs = {'config' : Config(**kwargs)}
searcher = Search(**kwargs)

application = tornado.web.Application([
    (r"/complete", CompleteHandler, dict(searcher=searcher)),
    (r"/search", SearchHandler, dict(searcher=searcher)),
])

if __name__ == "__main__":
    application.listen(80)
    tornado.ioloop.IOLoop.instance().start()
