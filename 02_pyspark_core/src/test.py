# 通过外部文件的方式来构建RDD
from pyspark import SparkContext, SparkConf
import os
import time
path = os.getcwd()

if __name__ == '__main__':
    # 创建SparkContext对象
    conf = SparkConf().setMaster("local[*]").setAppName("create_rdd_testfile")
    sc = SparkContext(conf=conf)
    rdd_init = sc.parallelize(['张三 李四 王五','赵六 田七 周八'])
    rdd_res = rdd_init.flatMap(lambda name: name.split())
    print(rdd_res.collect())
    # ['张三', '李四', '王五', '赵六', '田七', '周八']