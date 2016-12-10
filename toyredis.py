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

    def _send(self, b):
        n = 0
        while (n < len(b)):
            n += self._sock.send(b[n:])

    def _recv(self, ln):
        while len(self._readbuf) < ln:
            b = self._sock.recv(ln-len(self._readbuf))
            if not b:
                raise socket.error("Can't recv packets")
            self._readbuf += b
        r = self._readbuf[:ln]
        self._readbuf = self._readbuf[ln:]
        return r

    def _recv_line(self):
        i = self._readbuf.find(b'\r\n')
        while i < 0:
            self._readbuf += self._sock.recv(1024)
            i = self._readbuf.find(b'\r\n')
        r = self._readbuf[:i]
        self._readbuf = self._readbuf[i+2:]
        return r

    def close(self):
        self._sock.close()

    def set(self, k, v):
        if isinstance(k, str):
            k = k.encode('utf-8')
        if isinstance(v, int):
            v = str(v)
        if isinstance(v, str):
            v = v.encode('utf-8')
        self._send(b'*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n' % (len(k), k, len(v), v))
        s = self._recv_line()
        if s != b'+OK':
            ValueError(s)

    def get(self, k):
        if isinstance(k, str):
            k = k.encode('utf-8')
        self._send(b'*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n' % (len(k), k))
        s = self._recv_line()
        ln = int(s[1:])
        return self._recv(ln+2)[:-2]

    def incr(self, k):
        if isinstance(k, str):
            k = k.encode('utf-8')
        self._send(b'*2\r\n$4\r\nINCR\r\n$%d\r\n%s\r\n' % (len(k), k))
        s = self._recv_line()
        return int(s[1:])

    def incrby(self, k, v):
        if isinstance(k, str):
            k = k.encode('utf-8')
        v = str(v).encode('utf-8')
        self._send(b'*3\r\n$6\r\nINCRBY\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n' % (len(k), k, len(v), v))
        s = self._recv_line()
        return int(s[1:])

def connect(host, port=6379):
    return RedisConnection(host, port)
