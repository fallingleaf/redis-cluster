from redis.connection import ConnectionPool
from redis.crc16 import key_to_slot
from redis.exceptions import (
    RedisError, ResponseError, InvalidResponse, ClusterError, ConnectionError
)
import random


class RedisDB(object):
    def get_pool(self, key=None):
        raise NotImplementedError

    def execute_command(self, *args, **kwargs):
        raise NotImplementedError


class SingleDB(RedisDB):
    def __init__(self, hosts, **kwargs):
        host = hosts[0]
        if isinstance(host, str):
            self.pool = ConnectionPool.from_url(host, **kwargs)
        else:
            db = host.get('db', 0)
            self.pool = ConnectionPool(host=host['host'],
                                       port=host['port'],
                                       db=db, **kwargs)

    def get_pool(self, key=None):
        return self.pool

    def execute_command(self, *args, **kwargs):
        pool = self.get_pool()
        return pool.execute_command(*args, **kwargs)


class RoundRobinDB(RedisDB):
    def __init__(self, hosts, **kwargs):
        self.pools = []
        for host in hosts:
            pool = ConnectionPool(host=host['host'],
                                  port=host['port'],
                                  **kwargs)
            self.pools.append(pool)
        self._len = len(pool)
        self.idx = 0

    def get_pool(self, key=None):
        pool = self.pools[self.idx % self._len]
        self.idx = (self.idx + 1) % self._len
        return pool

    def execute_command(self, *args, **kwargs):
        pool = self.get_pool()
        return pool.execute_command(*args, **kwargs)

    def __str__(self):
        return '\n'.join([str(p) for p in self.pools])


class Cluster(RedisDB):
    def __init__(self, hosts, **kwargs):
        self.mapping = {}
        self.cluster_pools = {}
        self.max_resets = kwargs.pop('max_resets', 2)
        self.init_pools(hosts, **kwargs)
        self.pool_kwargs = kwargs

    def parse_addr(self, host):
        idx = host.find('@')
        if idx == -1:
            return ''

        if '?' in host:
            e = host.rfind('?')
            return host[idx+1: e]
        else:
            e = host.rfind('/')
            return host[idx+1: e]

    def init_pools(self, hosts, **kwargs):
        # Ensure consistency of db value
        kwargs.update({'db': 0})
        for host in hosts:
            if isinstance(host, str):
                addr = self.parse_addr(host)
                pool = ConnectionPool.from_url(host, **kwargs)
            else:
                addr = '{}:{}'.format(host['host'], host['port'])
                pool = ConnectionPool(host=host['host'], port=host['port'],
                                      **kwargs)
            self.cluster_pools[addr] = pool

        self.reset_slots(**kwargs)

    def get_random_pool(self):
        pools = list(self.cluster_pools.values())
        idx = random.randint(0, len(pools) - 1)
        return pools[idx]

    def _stringconv(self, t):
        if isinstance(t, bytes):
            return t.decode('utf-8')
        return t

    def reset_slots(self, **kwargs):
        pool = self.get_random_pool()
        resp = pool.execute_command('CLUSTER', 'SLOTS')
        if not isinstance(resp, (list, tuple)):
            raise RedisError('Unable to locate redis slots.')

        _pools = {}
        for elem in resp:
            _start, _end, master = elem[0], elem[1], elem[2]
            ip, port = self._stringconv(master[0]), self._stringconv(master[1])
            addr = '{}:{}'.format(ip, port)

            for i in range(int(_start), int(_end) + 1):
                self.mapping[i] = addr

            if addr not in _pools:
                p = (self.cluster_pools.pop(addr, None) or
                     ConnectionPool(host=ip, port=port, **kwargs))
                p.addr = addr
                _pools[addr] = p

        self.cluster_pools.clear()
        self.cluster_pools = _pools

    def _key_to_addr(self, key):
        slot = key_to_slot(key)
        return self.mapping.get(slot)

    def get_pool(self, key=None):
        if not key:
            pool = self.get_random_pool()
            return pool
        addr = self._key_to_addr(key)
        if addr:
            pool = self.cluster_pools.get(addr)
            if not pool:
                host, port = addr.split(':')
                pool = ConnectionPool(host=host, port=int(port),
                                      **self.pool_kwargs)
                self.cluster_pools[addr] = pool
            return pool
        raise ClusterError("No slot found for key...", key)

    def redirect_info(self, msg):
        parts = msg.split()
        if len(parts) < 3:
            return None, None
        return parts[1], parts[2]

    def execute_command(self, *args, **kwargs):
        pool = kwargs.pop('pool', None)
        ask = kwargs.pop('ask', False)

        if not pool:
            if len(args) < 2:
                pool = self.get_pool()
            else:
                key = str(args[1])
                pool = self.get_pool(key=key)
        try:
            if ask:
                pool.execute_command('ASKING')
                ask = False
            response = pool.execute_command(*args, **kwargs)
            return response

        except (ConnectionError, ResponseError) as e:
            msg = str(e)
            ask = msg.startswith('ASK ')
            moved = msg.startswith('MOVED ')

            num_resets = kwargs.pop('num_resets', 0)
            if num_resets > self.max_resets:
                raise ClusterError('Too many resets happened.')

            # Disconnect current pool and remove pool from cluster
            current_addr = pool.addr
            pool.disconnect()
            self.cluster_pools.pop(current_addr, None)

            self.reset_slots()
            num_resets += 1
            kwargs.update({'num_resets': num_resets})

            if ask or moved:
                _, addr = self.redirect_info(msg)

                if not addr or addr not in self.cluster_pools:
                    raise InvalidResponse('Invalid redirect node %s.' % addr)

                pool = self.cluster_pools[addr]

                kwargs.update({'ask': ask,
                               'pool': pool})

            elif isinstance(e, ResponseError):
                raise

            return self.execute_command(*args, **kwargs)

    def __str__(self):
        addrs = self.cluster_pools.keys()
        return '\n'.join(addrs)

    __repr__ = __str__
