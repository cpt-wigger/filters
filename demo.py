"""
过滤器测试，按需求实例化filter即可
Filters Demo
"""
from mem_filter import MemFilter
from redis_filter import RedisFilter
from mysql_filter import MysqlFilter
from bloom_filter import BloomFilter

# Test ram filter 测试内存过滤器
filter = MemFilter()

# Test redis filter 测试redis过滤器
# filter = RedisFilter()

# Test mysql filter 测试mysql过滤器
# mysql_host = "mysql+pymysql://root:wdxzdd@localhost:3306/filter?charset=utf8"
# filter = MysqlFilter(mysql_host=mysql_host, mysql_table_name="filter2")

# Test bloom_redis filter 测试布隆过滤器
# filter = BloomFilter(redis_key='bloom1')


def filter_test(test_data):
    save_count = 0
    ignore_count = 0
    for d in test_data:
        if filter.is_exists(d):
            ignore_count += 1
            print('data filtered：%s' % d)
        else:
            filter.save(d)
            save_count += 1
            print('saved:%s' % d)
    print("Success：%s，Filtered:%s" % (save_count, ignore_count))


if __name__ == '__main__':
    test_data = ['MON', 'TUES', 'MON', 'JAN', 'WED', 'SUN', 'SUN', 'SAT', 'TUES', 'FRI', 'FEB', 'JAN', 'MA']
    filter_test(test_data)
