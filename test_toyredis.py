#!/usr/bin/env python3
# coding:utf-8
###############################################################################
# MIT License
#
# Copyright (c) 2016-2017 Hajime Nakagami
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################
import unittest
import toyredis

class TestRedis(unittest.TestCase):
    host = '127.0.0.1'

    def test_type_independent(self):
        conn = toyredis.connect(self.host)

        # EXISTS
        conn.flushdb()
        self.assertEqual(conn.exists('foo'), False)
        conn.set('foo', 'bar')
        self.assertEqual(conn.exists('foo'), True)

        conn.close()

    def test_string(self):
        conn = toyredis.connect(self.host)

        # GETSET/GET/SET/SETNX
        conn.flushdb()
        self.assertEqual(conn.getset('foo', 'bar'), None)
        self.assertEqual(conn.getset('foo', 'baz'), 'bar')
        conn.delete('foo')

        self.assertEqual(conn.get('foo'), None)
        self.assertEqual(conn.setnx('foo', 'bar'), 1)
        self.assertEqual(conn.get('foo'), 'bar')
        conn.set('foo', 'baz')
        self.assertEqual(conn.get('foo'), 'baz')
        self.assertEqual(conn.setnx('foo', 'bar'), 0)
        self.assertEqual(conn.get('foo'), 'baz')
        conn.append('foo', '123')
        self.assertEqual(conn.substr('foo', 1, 4), 'az12')

        conn.set('kanji_character', '漢字')
        self.assertEqual(conn.get('kanji_character'), '漢字')

        b = '012\x00\x0d\x0a\x01\x02'
        conn.set('binary_data', b)
        self.assertEqual(conn.get('binary_data'), b)

        # SETEX/PSETEX
        conn.psetex('psetex_test', 1, "psetex")
        conn.setex('setex_test', 10, "setex")
        self.assertTrue(conn.ttl('setex_test') > 0)
        self.assertEqual(conn.ttl('psetex_test'), 0)

        # MSET/MGET
        conn.mset({'mkey1': 'value1', 'mkey2': 'value2'})
        self.assertEqual(conn.mget('mkey1', 'mkey2'), ['value1', 'value2'])


        # INCR/INCRBY
        conn.set('int_test', 1)
        self.assertEqual(conn.incr('int_test'), 2)
        self.assertEqual(conn.incrby('int_test', 3), 5)
        self.assertEqual(conn.decr('int_test'), 4)
        self.assertEqual(conn.decrby('int_test', 2), 2)

        conn.close()

    def test_list(self):
        conn = toyredis.connect(self.host)

        # List
        conn.flushdb()

        self.assertEqual(conn.rpush('list_test', 'foo'), 1)
        self.assertEqual(conn.lpush('list_test', 'bar'), 2)
        self.assertEqual(conn.rpush('list_test', 'baz'), 3)
        self.assertEqual(conn.llen('list_test'), 3)
        self.assertEqual(conn.lrange('list_test', 1, 2), ['foo', 'baz'])
        conn.ltrim('list_test', 1, 2)
        self.assertEqual(conn.lrange('list_test', 0, 1), ['foo', 'baz'])

        # sort
        self.assertEqual(conn.lpush('sort_test', 10), 1)
        self.assertEqual(conn.lpush('sort_test', 100), 2)
        self.assertEqual(conn.lpush('sort_test', 1), 3)
        self.assertEqual(conn.command('SORT sort_test DESC'), ['100', '10', '1'])

        conn.close()

    def test_set(self):
        conn = toyredis.connect(self.host)
        # Set
        conn.flushdb()
        conn.sadd('set_test', 'foo')
        conn.sadd('set_test', 'bar')
        conn.srem('set_test', 'bar')
        with self.assertRaises(ValueError):
            conn.srem('set_test', 'bar')
        self.assertEqual(conn.spop('set_test'), 'foo')
        self.assertEqual(conn.spop('set_test'), None)

        conn.close()

    def test_sorted_set_test(self):
        conn = toyredis.connect(self.host)
        # Sorted Set
        conn.flushdb()
        conn.zadd('set_test', 2, 'foo')
        conn.zadd('set_test', 1, 'bar')
        conn.zrem('set_test', 'bar')
        with self.assertRaises(ValueError):
            conn.zrem('set_test', 'bar')

        conn.close()

    def test_hash(self):
        conn = toyredis.connect(self.host)
        # Hash
        conn.flushdb()
        conn.hset('hash_test1', 'foo', 'FOO')
        conn.hset('hash_test1', 'bar', 'BAR')
        conn.hset('hash_test2', 'foo', 'Foo')
        conn.hset('hash_test2', 'bar', 'Bar')
        self.assertEqual(conn.hget('hash_test1', 'foo'), 'FOO')
        self.assertEqual(conn.hget('hash_test2', 'bar'), 'Bar')
        conn.hmset('hash_test1', {'bar': 'bar', 'baz': 'BAZ'})
        self.assertEqual(conn.hmget('hash_test1', 'foo', 'bar', 'baz'), ['FOO', 'bar', 'BAZ'])

        self.assertEqual(conn.hgetall('hash_test2'), {'foo': 'Foo', 'bar': 'Bar'})

        self.assertEqual(conn.hexists('hash_test2', 'foo'), True)
        self.assertEqual(conn.hexists('hash_test2', 'baz'), False)

        self.assertEqual(conn.hdel('hash_test2', 'foo'), True)
        self.assertEqual(conn.hdel('hash_test2', 'baz'), False)
        self.assertEqual(conn.hexists('hash_test2', 'foo'), False)

        self.assertEqual(conn.hlen('hash_test1'), 3)
        self.assertEqual(conn.hkeys('hash_test1'), ['foo', 'bar', 'baz'])
        self.assertEqual(conn.hvals('hash_test1'), ['FOO', 'bar', 'BAZ'])

        conn.close()


if __name__ == "__main__":
    unittest.main()
