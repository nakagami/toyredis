#!/usr/bin/env python3
# coding:utf-8
###############################################################################
# MIT License
#
# Copyright (c) 2016 Hajime Nakagami
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
        self.assertEqual(conn.getset('foo', 'baz'), b'bar')
        conn.delete('foo')

        self.assertEqual(conn.get('foo'), None)
        self.assertEqual(conn.setnx('foo', 'bar'), 1)
        self.assertEqual(conn.get('foo'), b'bar')
        conn.set('foo', 'baz')
        self.assertEqual(conn.get('foo'), b'baz')
        self.assertEqual(conn.setnx('foo', 'bar'), 0)
        self.assertEqual(conn.get('foo'), b'baz')
        conn.set('kanji_character', '漢字')
        self.assertEqual(conn.get('kanji_character'), b'\xe6\xbc\xa2\xe5\xad\x97')
        self.assertEqual(conn.get('kanji_character').decode('utf-8'), '漢字')

        # SETEX/PSETEX
        conn.psetex('psetex_test', 1, "psetex")
        conn.setex('setex_test', 10, "setex")
        self.assertTrue(conn.ttl('setex_test') > 0)
        self.assertEqual(conn.ttl('psetex_test'), 0)

        # MSET/MGET
        conn.mset({'mkey1': 'value1', 'mkey2': 'value2'})
        self.assertEqual(conn.mget('mkey1', 'mkey2'), [b'value1', b'value2'])


        # INCR/INCRBY
        conn.set('int_test', 1)
        self.assertEqual(conn.incr('int_test'), 2)
        self.assertEqual(conn.incrby('int_test', 3), 5)

        conn.close()

    def test_list(self):
        conn = toyredis.connect(self.host)

        # List
        conn.flushdb()
        self.assertEqual(conn.lpush('list_test', 'foo'), 1)
        self.assertEqual(conn.lpush('list_test', 'bar'), 2)
        self.assertEqual(conn.lpush('list_test', 'baz'), 3)

        conn.close()

if __name__ == "__main__":
    unittest.main()
