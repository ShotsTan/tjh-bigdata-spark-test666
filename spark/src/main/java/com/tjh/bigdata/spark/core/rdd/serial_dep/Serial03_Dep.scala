package com.tjh.bigdata.spark.core.rdd.serial_dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Opra_Action01
 * @Description: TODO 算子的分类
 * @Author: Tanjh
 * @Date: 2023/01/05 22:21
 * @Company: Copyright©
 * */
object Serial03_Dep {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Serial").set("spark.serializer",
      "org.apache.spark.serializer.KryoSerializer")
    // 注册需要使用 kryo 序列化的自定义类
    //      .registerKryoClasses(Array(classOf[Searcher]))
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List("hello Hadoop", "Scala Spark"), 2)
    println(rdd1.dependencies)
    println("**" * 30)
    val rdd2: RDD[String] = rdd1.flatMap(_.split(" "))
    println(rdd2.dependencies)
    println("**" * 30)
    val rdd3: RDD[(String, Int)] = rdd2.map((_, 1))
    println(rdd3.dependencies)
    println("**" * 30)
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey(_ + _)
    println(rdd4.dependencies)
    println("**" * 30)
    rdd4.collect()
    sc.stop()
  }

}
