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
import socket

__version__ = '0.0.1'

class RedisConnection:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self.host, self.port))
        self._readbuf = b''

    def _send_command(self, params):
        # params: sequence of bytes
        buf = b'*%d\r\n' % (len(params),)
        for p in params:
            buf += b'$%d\r\n%s\r\n' % (len(p), p)
        n = 0
        while (n < len(buf)):
            n += self._sock.send(buf[n:])

    def _recv_len(self, ln):
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

    def _recv(self):
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
            return self._recv_len(ln)
        elif r[0:1] == b'*':
            ln = int(r[1:])
            return [self._recv() for i in range(ln)]

        raise ValueError(r)

    def close(self):
        self._sock.close()

    def set(self, k, v):
        if isinstance(k, str):
            k = k.encode('utf-8')
        if isinstance(v, int):
            v = str(v)
        if isinstance(v, str):
            v = v.encode('utf-8')
        self._send_command([b'SET', k, v])

        assert self._recv() == b'OK'

    def delete(self, k):
        if isinstance(k, str):
            k = k.encode('utf-8')
        self._send_command([b'DEL', k])

        return self._recv()

    def get(self, k):
        if isinstance(k, str):
            k = k.encode('utf-8')
        self._send_command([b'GET', k])

        return self._recv()

    def incr(self, k):
        if isinstance(k, str):
            k = k.encode('utf-8')
        self._send_command([b'INCR', k])

        return self._recv()

    def incrby(self, k, v):
        if isinstance(k, str):
            k = k.encode('utf-8')
        v = str(v).encode('utf-8')
        self._send_command([b'INCRBY', k, v])

        return self._recv()

    def lpush(self, k, v):
        if isinstance(k, str):
            k = k.encode('utf-8')
        if isinstance(v, int):
            v = str(v)
        if isinstance(v, str):
            v = v.encode('utf-8')
        self._send_command([b'LPUSH', k, v])

        return self._recv()


def connect(host, port=6379):
    return RedisConnection(host, port)
