"""
基于内存进行去重，非持久
"""
from base import BaseFilter



class MemFilter(BaseFilter):
    def _get_storage(self):
        return set()

    def _save(self, hash_value):
        """
        利用集合进行存储
        :param hash_value:
        :return:
        """
        return self.storage.add(hash_value)

    def _is_exists(self, hash_value):
        if hash_value in self.storage:
            return True
        return False


