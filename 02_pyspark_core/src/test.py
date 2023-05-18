# 通过外部文件的方式来构建RDD
from pyspark import SparkContext, SparkConf
import os
import time
path = os.getcwd()

if __name__ == '__main__':
    # 创建SparkContext对象
    conf = SparkConf().setMaster("local[*]").setAppName("create_rdd_testfile")
    sc = SparkContext(conf=conf)
    rdd_init = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    rdd_res = rdd_init.foreach(lambda x: print("ok") if x >= 5 else print("error"))