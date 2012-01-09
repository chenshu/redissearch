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
            self.search('title', prefix, 10, self.async_callback(self.on_response))
        except UnicodeDecodeError, e:
            self.write(json_encode({'error' : 'cannot decode character'}))
            self.finsh()

    def search(self, t, prefix, count, callback):
        results = self.searcher.complete('title', prefix, count)
        callback(results)

    def on_response(self, response):
        self.write(json_encode(response))
        self.finish()

class SearchHandler(tornado.web.RequestHandler):

    def initialize(self, searcher):
        self.searcher = searcher

    @tornado.web.asynchronous
    def get(self):
        try:
            prefix = self.get_argument('q', None)
            self.search('title', prefix, 30, self.async_callback(self.on_response))
        except UnicodeDecodeError, e:
            self.write(json_encode({'error' : 'cannot decode character'}))
            self.finsh()

    def search(self, t, prefix, count, callback):
        results = self.searcher.query('title', prefix, count)
        callback(results)

    def on_response(self, response):
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
