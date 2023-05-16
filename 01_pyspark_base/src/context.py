from pyspark import SparkConf, SparkContext
import os
if __name__ == '__main__':
    conf = SparkConf().setMaster("local[5]").setAppName("test")
    sc = SparkContext(conf=conf)

    # 构建RDD对象
    rdd_init = sc.parallelize(["张三", "李四", "赵六", "田七", "郭五"], 2)

    # 如何查看RDD中每个分区的数据, 以及分区的数量
    print(rdd_init.getNumPartitions())
    # 4 根据参数local[*] 和sc.parallelize指定的分区数来确定分区数量
    # sc.parallelize指定的分区数的优先级大于local[*]指定的分区数优先级
    print(rdd_init.glom().collect())
    # [['张三'], ['李四'], ['赵六'], ['田七', '郭五']]

    # 给每个姓名后面都加上10
    rdd_map01 = rdd_init.map(lambda name: name+"10")
    print(rdd_map01.collect())
    # ['张三10', '李四10', '赵六10', '田七10', '郭五10']