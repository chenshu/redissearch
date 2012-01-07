#!/usr/bin/env python
#-*- coding: utf-8 -*-

import tornado.ioloop
import tornado.web
import tornado.httpclient
import simplejson as sj

class CompleteHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        try:
            prefix = self.get_argument('q')
            self.write(prefix)
        except UnicodeDecodeError, e:
            self.write(sj.dumps({'error' : 'cannot decode character'}))
        self.finish()

    def on_response(self, response):
        if response.error: raise tornado.web.HTTPError(500)
        json = tornado.escape.json_decode(response.body)
        self.write("Fetched " + str(len(json["entries"])) + " entries from the FriendFeed API")
        self.finish()

class SearchHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        self.write("Hello, world")
        http = tornado.httpclient.AsyncHTTPClient()
        http.fetch("http://friendfeed-api.com/v2/feed/bret", callback=self.on_response)

    def on_response(self, response):
        if response.error: raise tornado.web.HTTPError(500)
        json = tornado.escape.json_decode(response.body)
        self.write("Fetched " + str(len(json["entries"])) + " entries from the FriendFeed API")
        self.finish()

application = tornado.web.Application([
    (r"/complete", CompleteHandler),
    (r"/search", SearchHandler),
])

if __name__ == "__main__":
    application.listen(80)
    tornado.ioloop.IOLoop.instance().start()
