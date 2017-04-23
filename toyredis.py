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
import sys
try:
    import usocket as socket
except:
    import socket

__version__ = '0.1.0'

class RedisConnection:
    def __init__(self, host, port, encoding):
        self.host = host
        self.port = port
        self.encoding = encoding
        self._sock = socket.socket()
        if sys.implementation.name == 'micropython':
            self._sock.connect(socket.getaddrinfo(self.host, self.port)[0][-1])
        else:
            self._sock.connect((self.host, self.port))
        self._readbuf = b''

    def recv_response(self):
        i = self._readbuf.find(b'\r\n')
        while i < 0:
            self._readbuf += self._sock.recv(1024)
            i = self._readbuf.find(b'\r\n')
        r = self._readbuf[:i]
        self._readbuf = self._readbuf[i+2:]
        if r[0:1] == b'+':
            v = r[1:]
            if self.encoding:
                v = v.decode(self.encoding)
            return v
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
            if self.encoding:
                r = r.decode(self.encoding)
            return r
        elif r[0:1] == b'*':
            ln = int(r[1:])
            return [self.recv_response() for i in range(ln)]
        raise ValueError(r)

    def command(self, params):
        if isinstance(params, str):
            params = params.split()
        for i in range(len(params)):
            if not isinstance(params[i], bytes):
                params[i] = str(params[i]).encode(self.encoding if self.encoding else 'utf-8')
        buf = b'*%d\r\n' % (len(params),)
        for p in params:
            buf += b'$%d\r\n%s\r\n' % (len(p), p)
        n = 0
        while (n < len(buf)):
            n += self._sock.send(buf[n:])
        return self.recv_response()

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
        return self.command([b'FLUSHDB']) == 'OK'

    # Commands for string

    def set(self, k, v):
        return self.command([b'SET', k, v]) == 'OK'

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
        return self.command(c) == 'OK'

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

    def lrange(self, k, start, end):
        return self.command([b'LRANGE', k, start, end])

    def ltrim(self, k, start, end):
        return self.command([b'LTRIM', k, start, end])

    def lindex(self, k, i):
        return self.command([b'LINDEX', k, i])

    def lset(self, k, i, v):
        return self.command([b'LSET', k, i, v]) == 'OK'

    def lrem(self, k, i, v):
        return self.command([b'LREM', k, i, v]) == 'OK'

    def lpop(self, k):
        return self.command([b'LPOP', k])

    def rpop(self, k):
        return self.command([b'LPOP', k])

    def blpop(self, k, vs, timeout):
        return self.command([b'BLPOP', k] + vs + [timeout])

    def brpop(self, k, vs, timeout):
        return self.command([b'BRPOP', k] + vs + [timeout])

    def rpoplpush(self, src_k, dst_k):
        return self.command([b'RPOPLPUSH', src_k, dst_k])


    # Commands for set

    def sadd(self, k, v):
        return self.command([b'SADD', k, v])

    def srem(self, k, v):
        r = self.command([b'SREM', k, v])
        if r == 0:
            raise ValueError('value not found')

    def spop(self, k):
        return self.command([b'SPOP', k])

    def smove(self, src_k, dst_k, v):
        return self.command([b'SMOVE', src_k, dst_k, v]) == 1

    def scard(self, k):
        return self.command([b'SCARD', k])

    def sismember(self, k, v):
        return self.command([b'SISMEMBER', k, v]) == 1

    def sinter(self, *ks):
        return self.command([b'SINTER'] + list(ks))

    def sinterstore(self, dst_k, *ks):
        return self.command([b'SINTERSTORE', dst_k] + list(ks))

    def sunion(self, *ks):
        return self.command([b'SUNION'] + list(ks))

    def sunionstore(self, dst_k, *ks):
        return self.command([b'SINTERSTORE', dst_k] + list(ks))

    def sdiff(self, *ks):
        return self.command([b'SUNION'] + list(ks))

    def sdiffstore(self, dst_k, *ks):
        return self.command([b'SINTERSTORE', dst_k] + list(ks))

    def smembers(self, k):
        return self.command([b'SMEMERS', k])

    def srandmember(self, k):
        return self.command([b'SRANDMEMBER', k])


    # Commands for sorted set

    def zadd(self, k, score, m):
        return self.command([b'ZADD', k, score, m])

    def zrem(self, k, m):
        r = self.command([b'ZREM', k, m])
        if r == 0:
            raise ValueError('value not found')

    def zincrby(self, k, i, m):
        return self.command([b'ZINCRBY', k, i, m])

    def zrank(self, k, m):
        return self.command([b'ZRANK', k, m])

    def zrevrank(self, k, m):
        return self.command([b'ZREVRANK', k, m])

    def zrange(self, k, start, end, withscores=False):
        if withscores:
            r = self.command([b'ZRANGE', k, start, end, 'WITHSCORES'])
            return [(v, int(score)) for v, score in zip(r[::2], r[1::2])]
        else:
            return self.command([b'ZRANGE', k, start, end])

    def zrevrange(self, k, start, end, withscores=False):
        if withscores:
            r = self.command([b'ZREVRANGE', k, start, end, 'WITHSCORES'])
            return [(v, int(score)) for v, score in zip(r[::2], r[1::2])]
        else:
            return self.command([b'ZREVRANGE', k, start, end])

    # TODO: zrangebyscore

    def zcount(self, k, min_m, max_m):
        return self.command([b'ZCOUNT', k, min_m, max_m])

    def zcard(self, k):
        return self.command([b'ZCARD', k])

    def zscore(self, k, e):
        return self.command([b'ZSCORE', k, e])

    def zremrangebyrank(self, k, start, end):
        return self.command([b'ZREMRANGEBYRANK', k, start, end])

    def zremrangebyscore(self, k, start, end):
        return self.command([b'ZREMRANGEBYSCORE', k, start, end])

    # TODO: zunionstore
    # TODO: zinterstore


    # Commands for hash

    def hset(self, k, f, v):
        return self.command([b'HSET', k, f, v])

    def hget(self, k, f):
        return self.command([b'HGET', k, f])

    def hmget(self, k, *fs):
        return self.command([b'HMGET', k] + list(fs))

    def hmset(self, k, d):
        assert isinstance(d, dict)
        c = [b'HMSET', k]
        for dk, dv in d.items():
            c.extend([dk, dv])
        return self.command(c) == 'OK'

    def hincrby(self, k, f, v):
        return self.command([b'HINCRBY', k, f, v])


    def hexists(self, k, f):
        return self.command([b'HEXISTS', k, f]) == 1

    def hdel(self, k, f):
        return self.command([b'HDEL', k, f]) == 1

    def hlen(self, k):
        return self.command([b'HLEN', k])

    def hkeys(self, k):
        return self.command([b'HKEYS', k])

    def hvals(self, k):
        return self.command([b'HVALS', k])

    def hgetall(self, k):
        r = self.command([b'HGETALL', k])
        return dict(zip(r[::2], r[1::2]))

    # Publish/Subscribe

    def subscribe(self, k):
        self.command([b'SUBSCRIBE', k])


def connect(host, port=6379, encoding='utf-8'):
    return RedisConnection(host, port, encoding=encoding)
