# 通过外部文件的方式来构建RDD
from pyspark import SparkContext, SparkConf
import os
path = os.getcwd()

if __name__ == '__main__':
    # 创建SparkContext对象
    conf = SparkConf().setMaster("local[*]").setAppName("create_rdd_testfile")
    sc = SparkContext(conf=conf)
    rdd_init = sc.wholeTextFiles(f"file://{path}/../data/*")
    rdd_init = 
    rdd_init =
    # print(f"{path}/../data/words.txt")
    #----------------------------------------------------
    # getNumPartitions 查看分区数量
    # 如果有多个文件则默认按照文件数量确定分区数量
    print(rdd_init.getNumPartitions())
    # 2
    #----------------------------------------------------
    # 查看分区后的数据
    # print(rdd_init.glom().collect())
    # rdd_init.saveAsTextFile("hdfs://node1:8020/tmp/output")
    # [[('file:/tmp/workspace/spark01/02_pyspark_core/data/a.txt', 'aa aa aa\nkaka kklt\nkaka kklt\nkaka ka\nguowei xihuijian chenxuehong\nliyaqin'), ('file:/tmp/workspace/spark01/02_pyspark_core/data/b.txt', 'bb bb bb\nkaka kklt\nkaka kklt\nkaka ka\nguowei xihuijian chenxuehong\nliyaqin')], [('file:/tmp/workspace/spark01/02_pyspark_core/data/c.txt', 'cc cc cc\nkaka kklt\nkaka kklt\nkaka ka\nguowei xihuijian chenxuehong\nliyaqin'), ('file:/tmp/workspace/spark01/02_pyspark_core/data/words.txt', 'kaka kklt\nkaka kklt\nkaka ka\nguowei xihuijian chenxuehong\nliyaqin')]]
    #
    # rdd_flat = rdd_init.flatMap(lambda line: line.split())
    # print(rdd_flat.collect())
    rdd_map = rdd_init.map(lambda line: line)
    # print(rdd_map.collect())
    # rdd_map01 = rdd_map.map(lambda line: line[1])
    # print(rdd_map01.collect())
    # # rdd_map01_flat = rdd_map01.flatMap(lambda line: line.spilt())
    # # print(rdd_map01_flat.collect())
    # rdd_res = rdd_map01.flatMap(lambda line: line.split().split("\n"))
    # print(rdd_res.collect())
