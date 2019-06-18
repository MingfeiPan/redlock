import collections
import time
import os

import redis


class GetLockFail(Exception):
    pass


class RedisException(Exception):
    pass


class Redlock:
    _retry = 3
    _clock_drift = 0.01
    _unlock_script = 'if redis.call("get",KEYS[1]) == ARGV[1] then return redis.call("del",KEYS[1]) else return 0 end'

    def __init__(self, node_list):

        self.nodes = []

        for node in node_list:
            if not isinstance(node, dict):
                raise TypeError('node should be formed as a dict')
            saved_node = redis.StrictRedis(**node)
            self.nodes.append(saved_node)

        self.quorum = (len(self.nodes) // 2) + 1

        if len(self.nodes) < len(node_list):
            raise GetLockFail('redis nodes init failed')

    def _get_unique_value(self):

        #I assume it’s 20 bytes from /dev/urandom
        return os.urandom(20)

    def lock(self, key, ttl):

        if not isinstance(ttl, int):
            raise TypeError('ttl should be int')

        retry = 0
        drift = int(ttl * self._clock_drift)
        value = self._get_unique_value()

        while retry < self._retry:
            locked_num = 0

            start_time = time.time()

            for node in self.nodes:
                try:
                    if node.set(key, value, nx=True, px=ttl):
                        locked_num += 1
                except RedisException as e:
                    raise e

            elapsed_time = int((time.time() - start_time) * 1000)
            validity = ttl - elapsed_time - drift
            if validity > 0 and locked_num >= self.quorum:
                return (validity, key, value)
            else:
                for node in self.nodes:
                    node.eval(self._unlock_script, 1, key, value)

            retry += 1
        return False

    def unlock(self, key, value):

        for node in self.nodes:
            try:
                node.eval(self._unlock_script, 1, key, value)
            except RedisException as e:
                raise e
