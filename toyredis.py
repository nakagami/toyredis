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
import sys
try:
    import usocket as socket
except:
    import socket

__version__ = '0.0.2'

class RedisConnection:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._sock = socket.socket()
        if sys.implementation.name == 'micropython':
            self._sock.connect(socket.getaddrinfo(self.host, self.port)[0][-1])
        else:
            self._sock.connect((self.host, self.port))
        self._readbuf = b''

    def recv(self):
        i = self._readbuf.find(b'\r\n')
        while i < 0:
            self._readbuf += self._sock.recv(1024)
            i = self._readbuf.find(b'\r\n')
        r = self._readbuf[:i]
        self._readbuf = self._readbuf[i+2:]
        if r[0:1] == b'+':
            return r[1:]
        elif r[0:1] == b'-':
            raise ValueError(r[1:].decode('utf-8'))
        elif r[0:1] == b':':
            return int(r[1:])
        elif r[0:1] == b'$':
            ln = int(r[1:])
            if ln == -1:
                return None
            ln += 2     # read with terminator b'\r\n'
            while len(self._readbuf) < ln:
                b = self._sock.recv(ln-len(self._readbuf))
                if not b:
                    raise socket.error("Can't recv packets")
                self._readbuf += b
            r = self._readbuf[:ln-2]
            assert self._readbuf[ln-2:ln] == b'\r\n'
            self._readbuf = self._readbuf[ln:]
            return r
        elif r[0:1] == b'*':
            ln = int(r[1:])
            return [self.recv() for i in range(ln)]
        raise ValueError(r)

    def command(self, params):
        if isinstance(params, str):
            params = params.split()
        for i in range(len(params)):
            if not isinstance(params[i], bytes):
                params[i] = str(params[i]).encode('utf-8')
        buf = b'*%d\r\n' % (len(params),)
        for p in params:
            buf += b'$%d\r\n%s\r\n' % (len(p), p)
        n = 0
        while (n < len(buf)):
            n += self._sock.send(buf[n:])
        return self.recv()

    def close(self):
        self._sock.close()


    # http://redis.shibu.jp/commandreference/

    # Type independent commands
    def exists(self, k):
        return self.command([b'EXISTS', k]) == 1

    def delete(self, k):
        return self.command([b'DEL', k])

    def ttl(self, k):
        return self.command([b'TTL', k])

    def flushdb(self):
        assert self.command([b'FLUSHDB']) == b'OK'

    # Commands for string

    def set(self, k, v):
        assert self.command([b'SET', k, v]) == b'OK'

    def get(self, k):
        return self.command([b'GET', k])

    def getset(self, k, v):
        return self.command([b'GETSET', k, v])

    def mget(self, *ks):
        return self.command([b'MGET'] + list(ks))

    def setnx(self, k, v):
        return self.command([b'SETNX', k, v])

    def setex(self, k, seconds, v):
        return self.command([b'SETEX', k, seconds, v])

    def psetex(self, k, milliseconds, v):
        return self.command([b'PSETEX', k, milliseconds, v])

    def mset(self, d):
        assert isinstance(d, dict)
        c = [b'MSET']
        for k, v in d.items():
            c.extend([k, v])
        assert self.command(c) == b'OK'

    def msetnx(self, k, values):
        return self.command([b'MSETNX', k] + values)

    def incr(self, k):
        return self.command([b'INCR', k])

    def incrby(self, k, v):
        return self.command([b'INCRBY', k, v])

    def decr(self, k):
        return self.command([b'DECR', k])

    def decrby(self, k, v):
        return self.command([b'DECRBY', k, v])

    def append(self, k, v):
        return self.command([b'APPEND', k, v])

    def substr(self, k, start, end):
        return self.command([b'SUBSTR', k, start, end])


    # Commands for list

    def rpush(self, k, v):
        return self.command([b'RPUSH', k, v])

    def lpush(self, k, v):
        return self.command([b'LPUSH', k, v])

    def llen(self, k):
        return self.command([b'LLEN', k])

    # TODO: lrange
    # TODO: ltrim
    # TODO: lindex
    # TODO: lset
    # TODO: lrem
    # TODO: lpop
    # TODO: rpop
    # TODO: blpop
    # TODO: brpop
    # TODO: droplpush

    # Commands for set

    # TODO: sadd
    # TODO: srem
    # TODO: spop
    # TODO: smove
    # TODO: scard
    # TODO: sismember
    # TODO: sinter
    # TODO: sinterstore
    # TODO: sunion
    # TODO: sunionstore
    # TODO: sdiff
    # TODO: sdiffstore
    # TODO: smembers
    # TODO: srandmember


    # Commands for sorted set

    # TODO: zadd
    # TODO: zrem
    # TODO: zincrby
    # TODO: zrank
    # TODO: zrevrank
    # TODO: zrange
    # TODO: zrevrange
    # TODO: zrangebyscore
    # TODO: zcount
    # TODO: zcard
    # TODO: zscope
    # TODO: zremrangebyrank
    # TODO: zremrangebyscore
    # TODO: zunionstore
    # TODO: zinterstore


    # Commands for hash

    # TODO: hset
    # TODO: hget
    # TODO: hmget
    # TODO: hmset
    # TODO: hincrby
    # TODO: hexists
    # TODO: hdel
    # TODO: hlen
    # TODO: hkeys
    # TODO: hvals
    # TODO: hgetall


    # Publish/Subscribe

    def subscribe(self, k):
        self.command([b'SUBSCRIBE', k])


def connect(host, port=6379):
    return RedisConnection(host, port)
