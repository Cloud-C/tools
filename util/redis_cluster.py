import redis


# redis 集群
class RedisCluster:
    """
        ex 1. 使用數字編號: hosts: <host>_0, <host>_1, ..., <host>_9

            rs_cluster = RedisCluster(
                clusters=10, host_base_name=<host>,
                host_base_password=<pw>, host_base_port=<port>
            )
            rs_cluster.set(_id=<id>, <key>, <value>)
            value = rs_cluster.get(_id=<id>, <key>)

        ex 2. 帶入host list:

            host_list = [
                (<host_name_1>, <password_1>, <port_1>),
                (<host_name_2>, <password_2>, <port_2>),
                (<host_name_3>, <password_3>, <port_3>),
                ...
            ]

            rs_cluster = RedisCluster(host_list=host_list)
            rs_cluster.set(_id=<id>, <key>, <value>)
            value = rs_cluster.get(_id=<id>, <key>)
    """
    def __init__(self, clusters=None, base_name=None, base_password=None, base_port=None, host_list=None):
        self.redis_list = list()
        self.redis_dict = dict()
        self.redis_count = None
        if clusters and base_name and base_password and base_port:
            self._init_with_numbers(clusters, base_name, base_password, base_port)
        elif host_list:
            self._init_with_list(host_list=host_list)
        if not (self.redis_dict and self.redis_list and self.redis_count):
            raise Exception('redis cluster init error')
        print('=== connect redis ===')
        print(self.redis_dict)
        print('=====================')

    def _get_redis(self, host, password, port):
        return redis.StrictRedis(host=host, port=port, password=password, charset='utf-8', decode_responses=True)

    def _init_with_numbers(self, clusters, base_name, base_password, base_port):
        self.redis_count = clusters
        temp_dict = dict()
        for i in range(clusters):
            host = f'{base_name}_{i}'
            port = int(base_port) + i
            password = base_password
            this_rs = self._get_redis(host=host, port=port, password=password)
            temp_dict.update({
                host: this_rs
            })
            self.redis_list.append(host)
        self.redis_dict.update(temp_dict)

    def _init_with_list(self, host_list):
        self.redis_count = len(host_list)
        temp_dict = dict()
        for (host, password, port) in host_list:
            this_rs = self._get_redis(host=host, port=port, password=password)
            temp_dict.update({
                host: this_rs
            })
            self.redis_list.append(host)
        self.redis_dict.update(temp_dict)

    def _find_rs(self, _id):
        serial_number = _id % self.redis_count
        redis_host_name = self.redis_list[serial_number]
        this_rs = self.redis_dict.get(redis_host_name)
        return this_rs

    def exists(self, _id, key):
        this_rs = self._find_rs(_id=_id)
        return this_rs.exists(key)

    def get(self, _id, key):
        this_rs = self._find_rs(_id=_id)
        return this_rs.get(key)

    def set(self, _id, key, value, ex=None, px=None, nx=False, xx=False):
        this_rs = self._find_rs(_id=_id)
        return this_rs.set(key, value, ex=ex, px=px, nx=nx, xx=xx)

    def hget(self, _id, head_key, key):
        this_rs = self._find_rs(_id=_id)
        return this_rs.hget(head_key, key)

    def hset(self, _id, head_key, key, value):
        this_rs = self._find_rs(_id=_id)
        return this_rs.hset(head_key, key, value)

    def hgetall(self, _id, head_key):
        this_rs = self._find_rs(_id=_id)
        return this_rs.hgetall(head_key)

    def hmset(self, _id, head_key, key_value_dict):
        this_rs = self._find_rs(_id=_id)
        return this_rs.hmset(head_key, key_value_dict)

    def expire(self, _id, key, ex):
        if not isinstance(ex, int):
            raise Exception('expire time type must be int. (seconds)')
        this_rs = self._find_rs(_id=_id)
        return this_rs.expire(key, time=ex)
