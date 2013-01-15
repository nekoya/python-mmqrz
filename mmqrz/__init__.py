# -*- coding: utf-8 -*-
"""
    mmqrz - MiniMum Queue for Redis Zset
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""
from time import time

from redis import Redis


class MmqrzException(Exception):
    pass


class _RedisStub(object):
    def __getattr__(self, name):
        raise MmqrzException('Call mmqrz.connect_db(host, port, database)'
                             ' in advance please.')
db = _RedisStub()


def connect_db(host, port, database):
    global db
    db = Redis(host=host, port=port, db=database)


class Mmqrz(object):
    _queues = 'mmqrz:queues'

    def qinit(self):
        for queue in self.qall():
            self.qrem(queue.name)
        db.delete(self._queues)

    def qadd(self, name, score=None):
        if score is None:
            score = 10
        db.zadd(self._queues, name, score)
        return self.qget(name)

    def qrem(self, name):
        queue = self.qget(name)
        queue.clear()
        db.zrem(self._queues, name)

    def qall(self):
        return self._select(-1)

    def qselect(self):
        for queue in self.qall():
            if queue.count():
                return queue
        raise IndexError('no jobs exists')

    def qget(self, name):
        for queue in self.qall():
            if queue.name == name:
                return queue
        raise IndexError('queue %s was not found' % name)

    def _select(self, range):
        return [Queue(*x) for x in
                db.zrevrange(self._queues, 0, range, withscores=True)]


class Queue(object):
    _prefix = 'mmqrz:queue'

    def __init__(self, name, score=10):
        self.name = name
        self.score = score
        self._key = ':'.join([self._prefix, name])

    def __repr__(self):
        return '%s : %s' % (self.name, self.score)

    def enqueue(self, task, score=None):
        if score is None:
            score = int(time())
        db.zadd(self._key, task, score)

    def dequeue(self):
        task = self._select(0)[0]
        db.zrem(self._key, task.value)
        return task

    def clear(self):
        db.delete(self._key)

    def count(self):
        return db.zcard(self._key)

    def all(self):
        return self._select(-1)

    def _select(self, range):
        return [Task(*x) for x in
                db.zrange(self._key, 0, range, withscores=True)]


class Task(object):
    def __init__(self, value, score):
        self.value = value
        self.score = score

    def __repr__(self):
        return '%s : %s' % (self.value, self.score)