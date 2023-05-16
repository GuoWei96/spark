from pyspark import SparkConf, SparkContext
import os
# if __name__ == '__main__':
def use_hdfs01():
    conf1 = SparkConf().setAppName("readfor").setMaster("yarn")
    sc = SparkContext(conf=conf1)
    rdd_init = sc.textFile("hdfs://node1:8020/kaka.txt")
    rdd_flatmap = rdd_init.flatMap(lambda line: line.split())
    rdd_map = rdd_flatmap.map(lambda word: (word, 1))
    rdd_res = rdd_map.reduceByKey(lambda x, y: x + y)
    rdd_res = rdd_res.map(lambda x: (x[1], x[0]))
    rdd_res= rdd_res.sortByKey(ascending=True)
    rdd_res = rdd_res.map(lambda x: (x[1], x[0]))
    # print(rdd_res.collect())
    # print(rdd_res.collect())
    # [('ka', 1), ('kklt', 2), ('kaka', 3)]
    # 将输出内若排序,默认升序,设置ascending=False则会降序
    rdd_sort= rdd_res.sortBy(lambda x: x[1], ascending=False)
    # print(rdd_sort.collect())
    # # 将文件存放在hdfs, 默认两个分区
    rdd_sort.saveAsTextFile("hdfs://node1:8020/rdd_res08.txt")

use_hdfs01()
def sss():
    a1: list = [('kaka', 3), ('kklt', 2), ('ka', 1)]
    a1.sort(key=lambda x: x[1], reverse=True)
    print(a1)

# sss()
