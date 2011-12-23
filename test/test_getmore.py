import time

import tornado.ioloop

import test_shunt
import eventually
import asyncmongo

class Batches:
    """
    Bookkeeping - record the batches of data we receive from the server so we
    can assert things about them.
    """
    def __init__(self, sizes):
        # Batch sizes to request from server
        self.sizes = sizes

        # Current batch
        self.index = 0

        # Batches of data received from server
        self.batches = []
        self.batch_timestamps = []

    def next_batch_size(self):
        if self.index < len(self.sizes):
            return self.sizes[self.index]
        else:
            return 0

    def append(self, batch):
        self.batches.append(batch)
        self.batch_timestamps.append(time.time())

    def sizes_received(self):
        return [len(batch) for batch in self.batches]

    def total_received(self):
        return sum(self.sizes_received())

class GetmoreTest(
    test_shunt.MongoTest,
    test_shunt.SynchronousMongoTest,
    eventually.AssertEventuallyTest
):
    """
    Test the GETMORE message:
    http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPGETMORE
    """
    n_documents = 300

    def setUp(self):
        super(GetmoreTest, self).setUp()
        self.pymongo_conn.test.drop_collection('foo')

        # Fun fact: if there's no index, and we call find() with a sort option,
        # then Mongo will have to do a full sort in memory, and since it's
        # loaded all the results into memory anyway, it will ignore the default
        # batch size of 101 results and just send us everything in one batch.
        self.pymongo_conn.test.foo.ensure_index([('i', asyncmongo.ASCENDING)])
        self.pymongo_conn.test.foo.insert([{'i': i} for i in xrange(self.n_documents)])

        self.db = asyncmongo.Client(
            pool_id='test_query', host='127.0.0.1',
            port=int(self.mongod_options[0][1]), dbname='test', mincached=3
        )

    def _test_cursor(self, sizes, expected_sizes, limit):
        """
        Test that we can do a cursor.get_more() with various batch sizes.
        @param sizes:   List of ints, the batch sizes to request in order
        @param limit:   Int, limit for initial query
        """
        batches = Batches(sizes)

        def callback(result, error):
            self.assertEqual(None, error, str(error))
            batches.append(result)

            if cursor.alive:
                # GETMORE
                cursor.get_more(callback, batch_size=batches.next_batch_size())
                batches.index += 1

        # Initial QUERY
        kwargs = dict(
            spec={},
            sort=[('i', asyncmongo.ASCENDING)],
            fields={'_id': False},
            callback=callback,
            batch_size=batches.next_batch_size(),
        )

        if limit is not None:
            kwargs['limit'] = limit

        cursor = self.db.foo.find(**kwargs)

        batches.index += 1

        self.assertNotEqual(None, cursor,
            "find() should return a Cursor instance"
        )

        self.assertEventuallyEqual(
            expected_sizes,
            batches.sizes_received, # A method that's called periodically
        )

        before = self.get_open_cursors()

        # This will complete once all the assertEventually calls complete.
        tornado.ioloop.IOLoop.instance().start()

        # check cursors
        after = self.get_open_cursors()
        self.assertEqual(
            before, after,
            "%d cursors left open (should be 0)" % (after - before),
        )

    def test_no_batch_size(self):
        # Arrange the query into a series of batches of peculiar sizes. The
        # final batch will have batch_size 0, meaning "send me the rest".
        self._test_cursor(
            sizes=[],
            expected_sizes=[101, self.n_documents - 101],
            limit=None,
        )

    def test_batch_sizes(self):
        # Arrange the query into a series of batches of peculiar sizes. The
        # final batch will have batch_size 0, meaning "send me the rest".
        sizes = [20, 17, 102, 1, 2, 4]
        self._test_cursor(
            sizes=sizes,
            expected_sizes=sizes + [self.n_documents - sum(sizes)],
            limit=None,
        )

    def test_big_limit(self):
        # Don't set a batch size, only a limit
        self._test_cursor(
            sizes=[],
            limit=1000,
            expected_sizes=[self.n_documents],
        )

    def test_medium_limit(self):
        self._test_cursor(
            sizes=[],
            limit=150,
            expected_sizes=[150],
        )

    def test_limit_and_batch_sizes(self):
        # Send a series of batch sizes and *also* a limit of 75 total records.
        self._test_cursor(
            sizes=[50, 1, 80, 7, 150],
            limit=75,
            expected_sizes=[50, 1, 24],
        )

    def test_tailable(self):
        self.pymongo_conn.test.drop_collection('capped_coll')
        self.pymongo_conn.test.create_collection(
            'capped_coll',
            size=10*1024**2, # 10 MB
            capped=True
        )

        self.pymongo_conn.test.capped_coll.insert(
            [{} for i in range(self.n_documents)],
            safe=True
        )

        before = self.get_open_cursors()

        def callback(result, error):
            tornado.ioloop.IOLoop.instance().stop()
#            print >> sys.stderr, 'got RESULTS:', len(result)
            self.assertEqual(None, error, str(error))
            self.assertEqual(expected_size, len(result))

        # Initial QUERY
        cursor = self.db.capped_coll.find(
            spec={},
            fields={'_id': False},
            tailable=True,
            callback=callback
        )

        # Default first batch size: 101 docs
        expected_size = 101

        self.assertNotEqual(None, cursor,
            "find() should return a Cursor instance"
        )

        # Get first batch
        tornado.ioloop.IOLoop.instance().start()

        # One open cursor
        self.assert_(cursor.alive)
        self.assertEqual(1, self.get_open_cursors() - before)

        # Get second batch -- everything remaining right now
        expected_size = self.n_documents - 101
        cursor.get_more(callback=callback)
        tornado.ioloop.IOLoop.instance().start()

        # Still one open cursor
        self.assert_(cursor.alive)
        self.assertEqual(1, self.get_open_cursors() - before)

        # Add to the capped collection
        expected_size = n_new_docs = 50
        self.pymongo_conn.test.capped_coll.insert(
            [{} for i in range(n_new_docs)], safe=True
        )

        # Let the cursor tail the next set of docs
        cursor.get_more(callback=callback)
        tornado.ioloop.IOLoop.instance().start()

        # Once more, add to the capped collection
        expected_size = n_new_docs = 250
        self.pymongo_conn.test.capped_coll.insert(
            [{} for i in range(n_new_docs)], safe=True
        )

        # Let the cursor tail the next set of docs
        cursor.get_more(callback=callback)
        tornado.ioloop.IOLoop.instance().start()

        # check cursors -- dereferencing the cursor here should delete it
        # server-side
        self.assertEqual(1, self.get_open_cursors() - before,
            "Should have 1 open cursor before deleting our reference to it"
        )

        del cursor
        after = self.get_open_cursors()
        self.assertEqual(
            before, after,
            "%d cursors left open (should be 0)" % (after - before),
        )

    def test_empty_capped_collection(self):
        # This might not be the behavior you expect from Mongo, but a tailable
        # cursor on an empty capped collection dies immediately. Verify it acts
        # this way in asyncmongo.
        self.pymongo_conn.test.drop_collection('capped_coll')
        self.pymongo_conn.test.create_collection(
            'capped_coll',
            size=10*1024**2, # 10 MB
            capped=True
        )

        before = self.get_open_cursors()

        def callback(result, error):
            tornado.ioloop.IOLoop.instance().stop()
            self.assertEqual(None, error, str(error))
            # Empty result
            self.assertEqual([], result)

        # Initial QUERY
        cursor = self.db.capped_coll.find(
            spec={},
            fields={'_id': False},
            tailable=True,
            await_data=True, # THIS IS WHAT WE'RE TESTING
            callback=callback
        )

        tornado.ioloop.IOLoop.instance().start()
        self.assertFalse(cursor.alive,
            "Tailable cursor on empty capped collection should die"
        )

        after = self.get_open_cursors()
        self.assertEqual(
            before, after,
            "%d cursors left open (should be 0)" % (after - before),
        )

    def test_await_data(self):
        self.pymongo_conn.test.drop_collection('capped_coll')
        self.pymongo_conn.test.create_collection(
            'capped_coll',
            size=10*1024**2, # 10 MB
            capped=True
        )

        # 10 documents
        self.pymongo_conn.test.capped_coll.insert(
            [{} for i in range(10)],
            safe=True
        )

        before = self.get_open_cursors()

        batches = Batches([])

        def callback(result, error):
            tornado.ioloop.IOLoop.instance().stop()
#            print >> sys.stderr, 'got RESULTS:', len(result)
            self.assertEqual(None, error, str(error))
            batches.append(result)

        # Initial QUERY
        cursor = self.db.capped_coll.find(
            spec={},
            fields={'_id': False},
            tailable=True,
            await_data=True, # THIS IS WHAT WE'RE TESTING
            callback=callback
        )

        self.assertEqual(
            (1 << 1) | (1 << 5), # tailable & await_data
            cursor._Cursor__query_options()
        )

        self.assertNotEqual(None, cursor,
            "find() should return a Cursor instance"
        )

        # Get first batch
        tornado.ioloop.IOLoop.instance().start()

        self.assertEqual(1, len(batches.batches))
        self.assertEqual(10, len(batches.batches[0]))

        # One open cursor
        self.assert_(cursor.alive, "Cursor should still be alive")
        self.assertEqual(1, self.get_open_cursors() - before)

        # Get more again -- there's nothing to get, but await_data should make
        # us pause a second or two before returning nothing
        cursor.get_more(callback=callback)
        start_time = time.time()
        tornado.ioloop.IOLoop.instance().start()

        self.assertEqual(2, len(batches.batches))
        self.assertEqual(0, len(batches.batches[1]))
        self.assertGreater(
            time.time() - start_time,
            1,
            "With await_data = True, cursor should wait at least a second"
            " before returning nothing"
        )

        # Still one open cursor
        self.assert_(cursor.alive)
        self.assertEqual(1, self.get_open_cursors() - before)

        del cursor
        after = self.get_open_cursors()
        self.assertEqual(
            before, after,
            "%d cursors left open (should be 0)" % (after - before),
        )

if __name__ == '__main__':
    import unittest
    unittest.main()
