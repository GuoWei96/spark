from pyspark import SparkConf, SparkContext
import os
if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)
    rdd_init = sc.parallelize(["guowei", "chenxuehong", "xihuijuan", "lifeifei", "liuyaqin"], 5)
    rdd_map = rdd_init.map(lambda name: name + "love")
    print(rdd_map.collect())