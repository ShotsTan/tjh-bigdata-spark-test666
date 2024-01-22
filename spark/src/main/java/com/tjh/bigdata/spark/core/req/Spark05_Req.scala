package com.tjh.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_Req
 * @Description: TODO Spark Core 案例
 * @Author: Tanjh
 * @Date: 2023/01/07 21:37
 * @Company: Copyright©
 * */
object Spark05_Req {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req")
    val sc = new SparkContext(config = conf)
//    val spark = new SparkSession(sc)

    // TODO 1.读取原始数据
    val rdd0: RDD[String] = sc.textFile("data/user.json")

//    rdd0.map(
//      json => {
//        val string: String = json.tail.init.toString
//        val attrs: Array[String] = string.split(",")
//        attrs.map(attr => {
//          val datas = attr.split(":")
//          val name = datas(0).trim
//          var value = datas(1).trim
//          var newValue = null
//          if (value.startsWith("\"")) {
//            newValue = value.substring(1, value.length - 1)
//          } else {
//            newValue = value.toInt
//          }
//          (name.substring(1, name.length - 1), newValue)
//        })
//        var gae = 0
//        (age, 1)
//      }
//    ).reduce(
//      (t1, t2) => {
//        (t1._1 + t2._1, t1._2 + t2._2)
//      }
//    )
//
//    println(total / cnt)



    sc.stop()

  }

}
