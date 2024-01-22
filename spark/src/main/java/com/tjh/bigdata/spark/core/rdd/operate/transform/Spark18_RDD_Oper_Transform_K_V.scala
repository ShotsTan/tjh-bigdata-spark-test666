package com.tjh.bigdata.spark.core.rdd.operate.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_RDD_Instance
 * @Description: TODO combineByKey
 * @Author: Tanjh
 * @Date: 2023/01/03 23:14
 * @Company: Copyright©
 * */
object Spark18_RDD_Oper_Transform_K_V {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Instance")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(("a", 1), ("b", 2), ("b", 3), ("a", 4), ("b", 5), ("a", 6)), 2)

    //    rdd.combineByKey()
    rdd.map {
      case (word, cnt) => {
        (word, (cnt, 1))
      }
    }.reduceByKey((t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2)
    }).mapValues {
      case (eord, cnt) => eord / cnt
    }.foreach(println)

    println("**" * 20)
    // 第一个参数: 转换第一个数据格式
    // 第二个参数:分区内计算
    // 第三个参数:分区间计算

    rdd.combineByKey(
      v => {(v, 1)
      },
      (t: (Int, Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        ((t1._1 + t2._1) , (t1._2 + t2._2))
      }
    ).mapValues(t=>t._1/t._2).foreach(println)

    rdd.combineByKey(
      x=>x,
      (x:Int,y) => x+y,
      (x:Int,y:Int) => x+y
    ).collect().foreach(println)

    sc.stop()
  }


}
