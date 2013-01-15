# coding: utf-8

from nose.tools import eq_, assert_true, assert_raises

import kauli.test.redis

from mmqrz import Mmqrz, Queue, MmqrzException, connect_db


def test_connect_db():
    mmqrz = Mmqrz()
    assert_raises(MmqrzException, mmqrz.qinit)


def test_queue():
    with kauli.test.redis.RedisServer() as tredis:
        connect_db('localhost', tredis.port, 0)

        queue = Queue('fruits')
        assert_true(isinstance(queue, Queue))
        eq_(queue.name, 'fruits')
        eq_(queue.score, 10)

        queue.enqueue('apple')
        queue.enqueue('banana')
        queue.enqueue('orange')
        eq_(queue.count(), 3)

        eq_(queue.dequeue().value, 'apple')
        eq_(queue.dequeue().value, 'banana')
        eq_(queue.dequeue().value, 'orange')
        assert_raises(IndexError, queue.dequeue)
        eq_(queue.count(), 0)

        # discard all tasks
        queue.enqueue('apple')
        queue.enqueue('banana')
        eq_(queue.count(), 2)
        queue.clear()
        eq_(queue.count(), 0)


def test_queue_withscore():
    def assert_task(task, value, score):
        eq_(task.value, value)
        eq_(task.score, score)

    with kauli.test.redis.RedisServer() as tredis:
        connect_db('localhost', tredis.port, 0)

        queue = Queue('mountains', 20)
        eq_(queue.score, 20)

        queue.enqueue('fuji', 3776)
        queue.enqueue('takao', 599)
        queue.enqueue('everest', 8848)

        assert_task(queue.dequeue(), 'takao', 599)
        assert_task(queue.dequeue(), 'fuji', 3776)
        assert_task(queue.dequeue(), 'everest', 8848)
        assert_raises(IndexError, queue.dequeue)


def test_mmqrz():
    def assert_queues(got, expect):
        eq_([(x.name, x.score) for x in got], expect)

    with kauli.test.redis.RedisServer() as tredis:
        connect_db('localhost', tredis.port, 0)

        mmqrz = Mmqrz()
        mmqrz.qinit()
        assert_queues(mmqrz.qall(), [])

        # add queues
        fruits = mmqrz.qadd('fruits')
        assert_true(isinstance(fruits, Queue))

        mmqrz.qadd('mountains', score=20)
        mmqrz.qadd('people', score=5)
        assert_queues(mmqrz.qall(),
                      [('mountains', 20.0), ('fruits', 10.0), ('people', 5.0)])

        # get queue
        people = mmqrz.qget('people')
        assert_true(isinstance(people, Queue))
        eq_(people.name, 'people')
        assert_raises(IndexError, mmqrz.qget, 'books')

        # remove queue
        mmqrz.qrem('fruits')
        assert_queues(mmqrz.qall(), [('mountains', 20.0), ('people', 5.0)])

        # qrem also delete tasks
        people.enqueue('Mike')
        eq_(people.count(), 1)
        mmqrz.qrem('people')
        eq_(mmqrz.qadd('people').count(), 0)


def test_mmqrz_select():
    with kauli.test.redis.RedisServer() as tredis:
        connect_db('localhost', tredis.port, 0)

        def setup(mmqrz):
            fruits = mmqrz.qadd('fruits', 200)
            fruits.enqueue('apple')

            people = mmqrz.qadd('people', 150)
            people.enqueue('Mike')

            mountains = mmqrz.qadd('mountains', 100)
            mountains.enqueue('fuji', 3776)
            mountains.enqueue('takao', 599)

        mmqrz = Mmqrz()
        mmqrz.qinit()

        setup(mmqrz)
        queue = mmqrz.qselect()
        eq_(queue.name, 'fruits')
        eq_(queue.dequeue().value, 'apple')

        queue = mmqrz.qselect()
        eq_(queue.name, 'people')
        eq_(queue.dequeue().value, 'Mike')

        queue = mmqrz.qselect()
        eq_(queue.name, 'mountains')
        eq_(queue.dequeue().value, 'takao')

        queue = mmqrz.qselect()
        eq_(queue.name, 'mountains')
        eq_(queue.dequeue().value, 'fuji')
