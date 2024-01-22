package test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_RDD_transform01
 * @Description: TODO RDD transform算子练习
 * @Author: Tanjh
 * @Date: 2023/01/09 14:12
 * @Company: Copyright©
 * */
object Spark_RDD_transform01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark_RDD_Transform_01")
    val sc = new SparkContext(conf)

    //    val rdd0: RDD[String] = sc.textFile("data/user_visit_action.txt")
    val rdd0 = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 4, 2, 6, 7, 8, 9, 7, 6), 3)
    val rdd2 = sc.makeRDD(List(12, 34, 5, 67, 8, 7, 3, 5, 6, 7, 8, 9, 7, 6, 19), 3)

    //    rdd0.cache()
    println(rdd0.getNumPartitions)

    // TODO 单值类型 :对单个RDD处理，处理后返回一个新的RDD
    // 1.map
    //    rdd0.map(x=>x.split("_").mkString("{",",","}")).collect().foreach(println)
    println(rdd0.map(x => (x, 1)).collect().toList)

    // 2.mapPartitions
    println(rdd0.mapPartitions(datas => {
      //可用Scala中集合函数
      datas.map(x => (x, 1))
    }).collect().toList)
    // 3.mapPartitionsWithIndex


    // 4.flatMap  --扁平化
    val rdd1 = sc.makeRDD(List(
      List(1, 2), List(3, 4)
    ), 1)
    println(rdd1.flatMap(x => x).collect().toList)


    //5.glom  --
    //    println(rdd0.glom().collect().foreach(println))

    //6.groupBy  --分组
    println(rdd0.groupBy(x => x % 3).collect().toList)

    //7.filter  --过滤
    println(rdd0.filter(x => x % 2 == 0).collect().toList)

    //8.sample  --抽样
    println(rdd0.sample(true, 4).collect().toList)

    //9.distinct --去重
    println(rdd0.distinct().collect().toList)

    //10.coalesce --缩减分区
    println(rdd0.coalesce(2).getNumPartitions)

    //11.repartition --重分区
    println(rdd0.repartition(4).getNumPartitions)

    //12.sortBy
    println(rdd0.sortBy(x => x % 2, false).collect().toList)

    // TODO 2.双 Value 类型 :两个RDD取交并补集

    println("-----双 Value 类型 :两个RDD取交并补集-----")
    // 1. intersection --交集
    println(rdd0.intersection(rdd2).collect().toList)

    // 2.union 并集
    println(rdd0.union(rdd2).collect().toList)

    //3. subtract 补集
    println(rdd0.subtract(rdd2).collect().toList)

    //4.zip
    println(rdd0.zip(rdd2).collect().toList)


    //TODO 3.Key - Value 类型
    //1.

    sc.stop()
  }

}
