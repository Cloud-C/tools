import redis
import random


class RedisDiversion(redis.StrictRedis):

    def __init__(self, size=5, **kwargs):
        super().__init__(**kwargs)
        self.size = size
        self.instances = []
        for i in range(size):
            self.instances.append(redis.StrictRedis(**kwargs))

    def exists(self, *args):
        instance = self.instances[random.randint(0, self.size - 1)]
        return instance.exists(*args)

    def get(self, *args, **kwargs):
        instance = self.instances[random.randint(0,self.size-1)]
        return instance.get(*args, **kwargs)

    def set(self, *args, **kwargs):
        instance = self.instances[random.randint(0, self.size - 1)]
        return instance.set(*args, **kwargs)

    def delete(self, *args, **kwargs):
        instance = self.instances[random.randint(0, self.size - 1)]
        return instance.delete(*args)

    def hget(self, *args, **kwargs):
        instance = self.instances[random.randint(0, self.size - 1)]
        return instance.hget(*args, **kwargs)

    def hmget(self, *args, **kwargs):
        instance = self.instances[random.randint(0, self.size - 1)]
        return instance.hmget(*args, **kwargs)

    def hset(self, *args, **kwargs):
        instance = self.instances[random.randint(0, self.size - 1)]
        return instance.hset(*args, **kwargs)

    def hmset(self, *args, **kwargs):
        instance = self.instances[random.randint(0, self.size - 1)]
        return instance.hmset(*args, **kwargs)

    def hgetall(self, *args, **kwargs):
        instance = self.instances[random.randint(0, self.size - 1)]
        return instance.hgetall(*args, **kwargs)

    def hdel(self, *args, **kwargs):
        instance = self.instances[random.randint(0, self.size - 1)]
        return instance.hdel(*args, **kwargs)
