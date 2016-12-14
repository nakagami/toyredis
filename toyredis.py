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
try:
    import usocket as socket
except:
    import socket

__version__ = '0.0.1'

class RedisConnection:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._sock = socket.socket()
        self._sock.connect(socket.getaddrinfo(self.host, self.port)[0][-1])
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
            raise ValueError(r[1:])
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

    def command_response(self, params):
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

    def set(self, k, v):
        assert self.command_response([b'SET', k, v]) == b'OK'

    def mset(self, k, values):
        return self.command_response([b'MSET', k] + values)

    def psetex(self, k, timeout, v):
        return self.command_response([b'PSETEX', k, timeout, v])

    def setex(self, k, timeout, v):
        return self.command_response([b'SETEX', k, timeout, v])

    def ttl(self, k):
        return self.command_response([b'TTL', k])

    def setnx(self, k, v):
        return self.command_response([b'SETNX', k, v])

    def msetnx(self, k, values):
        return self.command_response([b'MSETNX', k] + values)

    def delete(self, k):
        return self.command_response([b'DEL', k])

    def get(self, k):
        return self.command_response([b'GET', k])

    def getset(self, k, v):
        return self.command_response([b'GETSET', k, v])

    def incr(self, k):
        return self.command_response([b'INCR', k])

    def incrby(self, k, v):
        return self.command_response([b'INCRBY', k, v])

    def lpush(self, k, v):
        return self.command_response([b'LPUSH', k, v])

    def subscribe(self, k):
        self.command_response([b'SUBSCRIBE', k])


def connect(host, port=6379):
    return RedisConnection(host, port)
