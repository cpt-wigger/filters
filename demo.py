"""
过滤器测试，按需求实例化filter即可
"""
from mem_filter import MemFilter
from redis_filter import RedisFilter
from mysql_filter import MysqlFilter
from bloom_filter import BloomFilter

# 测试内存过滤器
# filter = MemFilter()

# 测试redis过滤器
# filter = RedisFilter()

# 测试mysql过滤器
# mysql_host = "mysql+pymysql://root:wdxzdd@localhost:3306/filter?charset=utf8"
# filter = MysqlFilter(mysql_host=mysql_host, mysql_table_name="filter2")

# 测试布隆过滤器
# filter = BloomFilter(redis_key='bloom1')


def filter_test(test_data):
    save_count = 0
    ignore_count = 0
    for d in test_data:
        if filter.is_exists(d):
            ignore_count += 1
            print('重复数据,已忽略：%s' % d)
        else:
            filter.save(d)
            save_count += 1
            print('成功保存:%s' % d)
    print("成功存储数据：%s条，重复过滤数据:%s条" % (save_count, ignore_count))


if __name__ == '__main__':
    test_data = ['马', 'fdf', '卢', '宋', '杰', '卢', '晗', '张', '宋', '春华', '了了', '天', '宁']
    filter_test(test_data)
