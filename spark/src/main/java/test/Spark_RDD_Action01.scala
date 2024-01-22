package test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_RDD_Action01
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/10 13:52
 * @Company: Copyright©
 * */
object Spark_RDD_Action01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark_RDD_Action01")
    val sc = new SparkContext(conf)
    val rdd0 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8), 3)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("e", 1), ("c", 3), ("a", 1), ("b", 4), ("a", 7), ("c", 2), ("c", 1)), 2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 34), ("c", 3), ("b", 4), ("a", 7), ("c", 2), ("c", 1)), 2)

    //    1) reduce
    println(rdd0.reduce(_ + _))

    //    2) collect
    rdd0.collect()

    //    3) count
    println(rdd2.count())

    //    4) first
    println(rdd2.first())

    //    5) take
    println(rdd0.take(3).toList)

    //    6) takeOrdered
    println(rdd0.takeOrdered(3).toList)

    //    7) aggregate
    println(rdd0.aggregate(0)((x, y) => Math.max(x, y), (x, y) => x + y))

    //    8) fold
    println(rdd0.fold(0)(_ + _))

    //    9) countByKey
    println(rdd2.countByKey())

    //    10) save相关算子
    //    rdd2.saveAsTextFile()
    //    rdd2.saveAsObjectFile()
    //    rdd2.saveAsSequenceFile()

    //    11) foreach
    rdd2.foreach(println)


  }

}
