# 通过外部文件的方式来构建RDD
from pyspark import SparkContext, SparkConf
import os
import time
path = os.getcwd()

if __name__ == '__main__':
    # 创建SparkContext对象
    conf = SparkConf().setMaster("local[*]").setAppName("create_rdd_testfile")
    sc = SparkContext(conf=conf)
    rdd_init = sc.textFile(f"file://{path}/../data/*")
    # print(f"{path}/../data/words.txt")
    #----------------------------------------------------
    # getNumPartitions 查看分区数量
    # 如果有多个文件则默认按照文件数量确定分区数量
    print(rdd_init.getNumPartitions())
    # 2
    #----------------------------------------------------
    # 查看分区后的数据
    print(rdd_init.glom().collect())
    # rdd_init.saveAsTextFile("hdfs://node1:8020/tmp/output")
    # [['kaka kklt', 'kaka kklt'], ['kaka ka']]

    rdd_flat = rdd_init.flatMap(lambda line: line.split())
    print(rdd_flat.collect())

    time.sleep(100)
    sc.stop()


#sc.wholeTextFiles尽可能的减少分区数量, 从而减少最终输出到目的文件的数量