# 使用redis 进行去重存储
from base import BaseFilter
import redis


class RedisFilter(BaseFilter):
    def _get_storage(self):
        """返回redis连接对象"""
        pool = redis.ConnectionPool(host=self.redis_host, port=self.redis_port, db=self.redis_db)
        client = redis.StrictRedis(connection_pool=pool)
        return client

    def _save(self, hash_value):
        """
        利用redis进行存储
        :param hash_value:
        :return:
        """
        return self.storage.sadd(self.redis_key, hash_value)

    def _is_exists(self, hash_value):
        """判断数据的哈希值是否在redis中已经存在"""
        return self.storage.sismember(self.redis_key, hash_value)
