from redis.connection import ConnectionPool
from redis.crc16 import key_to_slot
from redis.exceptions import (
    RedisError
)


class RedisDB(object):
    def get_pool(self, key=None):
        raise NotImplementedError

    def execute_command(self, *args, **kwargs):
        raise NotImplementedError


class SingleDB(RedisDB):
    def __init__(self, hosts, **kwargs):
        host = hosts[0]
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
        self.init_pools(hosts, **kwargs)
        self.pool_kwargs = kwargs

    def parse_addr(self, host):
        idx = host.find('@')
        if not idx:
            return ''

        if '?' in host:
            return host[idx+1: -5]
        return host[idx+1: -2]

    def init_pools(self, hosts, **kwargs):
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
        return pools[0]

    def reset_slots(self, **kwargs):
        pool = self.get_random_pool()
        resp = pool.execute_command('CLUSTER', 'SLOTS')
        if not isinstance(resp, (list, tuple)):
            raise RedisError('Unable to locate redis slots.')

        for elem in resp:
            _start, _end, master = elem[0], elem[1], elem[2]
            ip, port = master[0], master[1]
            addr = '{}:{}'.format(ip, port)

            for i in range(int(_start), int(_end) + 1):
                self.mapping[i] = addr

            if addr not in self.cluster_pools:
                self.cluster_pools[addr] = ConnectionPool(host=ip,
                                                          port=port,
                                                          **kwargs)

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
        raise RedisError("No slot found for key...", key)

    def execute_command(self, *args, **kwargs):
        if len(args) < 2:
            pool = self.get_pool()
        else:
            key = str(args[1])
            pool = self.get_pool(key=key)
        return pool.execute_command(*args, **kwargs)

    def __str__(self):
        addrs = self.cluster_pools.keys()
        return '\n'.join(addrs)

    __repr__ = __str__


if __name__ == '__main__':
    cluster = Cluster(['redis://@10.150.92.50:6379/0'])
    print("list of cluster nodes...\n", cluster)
    cluster.set('CME3_CACHE_QUERY_DOCUMENT_WWW_CUTENESS_COM', 'abc123')
    cluster.set('{CME3_CACHE}_GOOGLE_KW_WWW_CUTENESS_COM', 'dogs')

    print(cluster.get('{CME3_CACHE}_QUERY_DOCUMENT_WWW_CUTENESS_COM'))
    print(cluster.get('{CME3_CACHE}_GOOGLE_KW_WWW_CUTENESS_COM'))
