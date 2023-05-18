from pyspark import SparkContext, SparkConf
import os
if __name__ == '__main__':
    # 创建SparkContext对象
    conf = SparkConf().setMaster("local[*]").setAppName("create_rdd_testfile")
    sc = SparkContext(conf=conf)
    rdd_init = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 1)

    # print(rdd_init.getNumPartitions())
    # # 4
    # print(rdd_init.glom().collect())
    # # [[1, 2], [3, 4], [5, 6], [7, 8, 9, 10]]
    #
    # rdd_res = rdd_init.map(lambda num: num + 1)
    # print(rdd_res.collect())
    # # [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    # def add(fn):
    #     arr = []
    #     for i in fn:
    #         arr.append(i + 1)
    #     return arr
    #
    # rdd_res = rdd_init.mapPartitions(add)
    # print(rdd_res.collect())

    # map的使用 对数据进行遍历打印
    rdd_res = rdd_init.map(lambda num: print(num))
    #
    def kaka(li):
        for i in li:
            print(i)
    rdd_res = rdd_init.mapPartitions(kaka)


