package com.tjh.bigdata.spark.core.req

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark01_Req
 * @Description: TODO Spark Core 案例
 * @Author: Tanjh
 * @Date: 2023/01/07 21:37
 * @Company: Copyright©
 * */
object Spark01_Req {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req")
    val sc = new SparkContext(config = conf)

    // TODO 1.读取原始数据
    val rdd0: RDD[String] = sc.textFile("data/user_visit_action.txt")
    rdd0.persist()


    // TODO 2.统计分析
    //  2.1 统计点击数量
    //点击数据
    val clickCnt = rdd0.filter(x => x.split("_")(6) != "-1").
      map(x => (x.split("_")(6), 1)).
      reduceByKey(_ + _)

    //  2.2 统计下单数量
    val orderCnt = rdd0.filter(x => x.split("_")(8) != "null").
      flatMap(x => {
        x.split("_")(8).split(",").map(x => (x, 1))
      }).
      reduceByKey(_ + _)

    //  2.3 统计支付数量
    val payCnt = rdd0.filter(x => x.split("_")(10) != "null").
      flatMap(x => {
        x.split("_")(10).split(",").map(x => (x, 1))
      }).
      reduceByKey(_ + _)

    //    val top10: Array[(String, Int)] = clickCnt.sortBy(_._2, ascending = false).take(10)
    //    top10.foreach(println)
    //    (品类，点击数量) (品类，下单数量) (品类，支付数量)
    val coRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCnt.cogroup(orderCnt, payCnt)
    val mapDatas: RDD[(String, (Int, Int, Int))] = coRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        val clickCnt = clickIter.headOption.getOrElse(0)
        val orderCnt = orderIter.headOption.getOrElse(0)
        val payCnt = payIter.headOption.getOrElse(0)
        (clickCnt, orderCnt, payCnt)

      }
    }
    mapDatas.sortBy(_._2,ascending = false).take(10).foreach(println)







    // TODO 3.对数据排序，取前10名
    sc.stop()

  }

}
