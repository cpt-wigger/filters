# 使用bloom过滤器和redis进行去重存储
import six

from base import BaseFilter
import redis


class BloomFilter(BaseFilter):

    def _get_hash_value(self, data):
        # 将同一个数据多次哈希得到不同的值，相当于多个哈希函数运算
        hash_func_obj = self.hash_func()
        hash_func_obj.update(self._get_bytes_data(data))
        # 将获取的16进制数转化为整数
        hash_value_1 = int(hash_func_obj.hexdigest(), 16)
        offset_1 = self._get_offset(hash_value_1)
        hash_func_obj.update(self._get_bytes_data("2"))
        hash_value_2 = int(hash_func_obj.hexdigest(), 16)
        offset_2 = self._get_offset(hash_value_2)
        hash_func_obj.update(self._get_bytes_data("3"))
        hash_value_3 = int(hash_func_obj.hexdigest(), 16)
        offset_3 = self._get_offset(hash_value_3)
        return offset_1, offset_2, offset_3

    def _get_offset(self, hash_value):
        # redis 的位数组最大长度为2**32
        offset = hash_value % (2 ** 32)
        return offset

    def _get_storage(self):
        """返回redis连接对象"""
        pool = redis.ConnectionPool(host=self.redis_host, port=self.redis_port, db=self.redis_db)
        client = redis.StrictRedis(connection_pool=pool)
        return client

    def _save(self, offsets):
        """
        利用redis进行存储
        :param hash_value:
        :return:
        """
        ret = []
        for offset in offsets:
            ret.append(self.storage.setbit(self.redis_key, offset, 1))
        return ret

    def _is_exists(self, offsets):
        """判断数据的哈希值是否在redis中已经存在"""
        ret = []
        for offset in offsets:
            ret.append(self.storage.getbit(self.redis_key, offset))
        if not all(ret):
            return False
        else:
            return True
