import six
import hashlib


class BaseFilter:
    """
    过滤器基类，用于继承
    """

    def __init__(self, hash_func="md5",
                 redis_host="localhost",
                 redis_port=6379,
                 redis_db=0,
                 mysql_host=None,
                 mysql_table_name="filter",
                 redis_key="filter"):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_key = redis_key
        self.mysql_host = mysql_host
        self.hash_func = getattr(hashlib, hash_func)
        self.mysql_table_name = mysql_table_name
        self.storage = self._get_storage()

    def _get_storage(self):
        """
        用于子类重写，返回对应的存储对象
        :return:
        """
        pass

    def _get_bytes_data(self, data):
        """
        将原始数据转为字节类型
        :param data:
        :return:
        """
        if six.PY3:
            if isinstance(data, bytes):
                return data
            elif isinstance(data, str):
                return data.encode()
            else:
                raise Exception("Type of data should be string!")
        else:
            if isinstance(data, str):
                return data
            elif isinstance(data, unicode):
                return data.encode()
            else:
                raise Exception("Type of data should be string!")

    def _get_hash_value(self, data):
        """
        计算哈希值
        :param data: 原始数据的字节类型
        :return: 哈希值
        """
        hash_func_obj = self.hash_func()
        hash_func_obj.update(self._get_bytes_data(data))
        hash_value = hash_func_obj.hexdigest()
        return hash_value

    def save(self, data):
        """
        计算指纹，进行存储
        :param data: 需要进行去重的数据
        :return: 存储结果
        """
        hash_value = self._get_hash_value(data)
        return self._save(hash_value)

    def _save(self, hash_value):
        """
        用于子类重写，使用指定方法存储哈希值
        :param hash_value:使用信息摘要算法计算的哈希值
        :return:存储结果
        """
        pass

    def is_exists(self, data):
        """
        判断数据是否存在
        :param data: 需要进行判断的数据
        :return: 存在/不存在  True/False
        """
        hash_value = self._get_hash_value(data)
        return self._is_exists(hash_value)

    def _is_exists(self, hash_value):
        """
        用于子类重写，查询判断哈希值是否存在
        :param hash_value: 需要判断的数据的哈希值
        :return: 存在/不存在  True/False
        """
        pass
