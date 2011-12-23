#!/usr/bin/env python
import functools

import json
import sys
import time

import tornado.ioloop
import tornado.web
import tornado.options

import pymongo
import pymongo.objectid
import asyncmongo

sync_db = None
async_db = None


def create_collection():
    sync_db.create_collection('capped_coll', size=10000, capped=True)
    print 'Created capped collection "capped_coll" in database "test"'


def json_default(obj):
    """
    Convert non-JSON-serializable obj to something serializable
    """
    if isinstance(obj, pymongo.objectid.ObjectId):
        return str(obj)
    else:
        raise TypeError("%s is not JSON-serializable" % repr(obj))


# Map client tokens (random floats) to MongoDB cursors
token2cursor = {}


class TailingHandler(tornado.web.RequestHandler):
    def _remove_dead_cursor(self):
        print 'Caught dead cursor'
        del token2cursor[self.token]

        # Make the client wait 1 second before trying again
        tornado.ioloop.IOLoop.instance().add_timeout(
            time.time() + 1,
            functools.partial(self._on_response, True, [], None)
        )

    def _find(self):
        # Start tailing capped_coll. The cursor dies immediately if the
        # collection is empty. Otherwise, it allows us to tail the cursor with
        # getMore messages.
        cursor = async_db.capped_coll.find(
            {}, tailable=True, await_data=True,
            callback=functools.partial(self._on_response, True)
        )

        token2cursor[self.token] = cursor

    def _get_more(self, cursor):
        # Continue tailing. The await_data=True option we passed in when we
        # created the cursor means that if there's no more data, the cursor
        # will wait about a second for more data to come in before returning
        cursor.get_more(
            callback=functools.partial(self._on_response, False)
        )

    @tornado.web.asynchronous
    def get(self, token):
        """
        Process a GET request for data from capped_coll. Returns a JSONified
        list of new documents as soon as they're available, or waits a few
        seconds to return if there aren't any new documents.
        """
        self.token = token

        cursor = token2cursor.get(token)
        if cursor and not cursor.alive:
            self._remove_dead_cursor()
        elif not cursor:
            self._find()
        else:
            self._get_more(cursor)

    def _on_response(self, new, response, error):
        """
        Asynchronous callback when find() or get_more() completes. Sends result
        to the client.
        @param new: Whether this is the response to a new cursor (find) or an
                    existing cursor (get_more)
        """
        if not self.request.connection.stream.socket:
            # Client went away
            if self.token in token2cursor:
                del token2cursor[self.token]

        elif error:
            # Something's wrong with this cursor, remove it
            if self.token in token2cursor:
                del token2cursor[self.token]

            # Ignore errors from dropped collections
            if error.message != 'cursor not valid at server':
                self.set_status(500)
                self.write(str(error))

            self.finish()

        elif not new and not response:
            # Response is empty list if no data came in during the seconds while
            # we waited for more data. get_more() does *not* block indefinitely
            # for more data, it only blocks for a few seconds. Let's not call
            # self.finish(), and just start waiting for data again.
            cursor = token2cursor.get(self.token)
            if cursor:
                if cursor.alive:
                    self._get_more(cursor)
                else:
                    self._remove_dead_cursor()
            else:
                self._find()
        else:
            # We have new data for the client.
            self.set_header('Content-Type', 'application/json; charset=UTF-8')
            final_response = json.dumps({
                'is_new': new,
                'response': response or []
            }, default=json_default)

            self.write(final_response)
            self.finish()


class DocumentHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def post(self):
        """
        Insert a new document to the capped collection
        """
        try:
            doc = json.loads(self.request.body)
            async_db.capped_coll.insert(doc, callback=self._on_response)
        except Exception, e:
            self.set_status(500)
            self.write(str(e))
            self.finish()

    def _on_response(self, response, error):
        if error:
            raise error
        self.finish()

class ClearCollectionHandler(tornado.web.RequestHandler):
    def post(self):
        """
        Delete everything in the collection
        """
        sync_db.capped_coll.drop()
        create_collection()


if __name__ == '__main__':
    sync_db = pymongo.Connection().test
    try:
        create_collection()
    except pymongo.errors.CollectionInvalid:
        if 'capped' not in sync_db.capped_coll.options():
            print >> sys.stderr, (
                'test.capped_coll exists and is not a capped collection,\n'
                'please drop the collection and start this example app again.'
            )
            sys.exit(1)

    tornado.options.parse_command_line()
    application = tornado.web.Application([
        # jQuery.ajax() with cache=false adds a random int to the end of the
        # URL like ?_=1234; ignore it
        (r'/capped_coll/(?P<token>[^?]+)', TailingHandler),
        (r'/document', DocumentHandler),
        (r'/clear', ClearCollectionHandler),
        (r'/(.*)', tornado.web.StaticFileHandler, {'path': 'index.html'})
    ])

    async_db = asyncmongo.Client(pool_id='test',
        host='127.0.0.1',
        port=27017,
        mincached=5,
        maxcached=15,
        maxconnections=30,
        dbname='test'
    )

    application.listen(8888)
    print 'Listening on port 8888'
    tornado.ioloop.IOLoop.instance().start()
