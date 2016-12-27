==========
toyredis
==========

Toy redis client

Requirement
------------

- Python 3.5+ https://www.python.org
- MicroPython https://micropython.org

Install
----------

::

   $ python3 -m pip install toyredis

Example
--------------

::

   >>> import toyredis
   >>> conn = toyredis.connect('localhost')
   >>> conn.set('foo', 'bar')
   >>> conn.get('foo')
   b'bar'
   >>> conn.set('foo', 10)
   >>> conn.incr('foo')
   11
   >>>
