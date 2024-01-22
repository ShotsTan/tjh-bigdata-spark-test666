package com.tjh.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @ClassName: Spark01_Acc
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/08 0:16
 * @Company: Copyright©
 * */
object Spark05_Acc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req")
    val sc = new SparkContext(config = conf)

    val rdd: RDD[String] = sc.makeRDD(List("a"), 3)

//    val sum: LongAccumulator = sc.longAccumulator("sum")

    // TODO 累加器实现wordcount
    //1.构建模型
    val acc = new MyAccumulator()

    //2.向Spark注册
    sc.register(acc)

    //3.使用模型
    rdd.foreach(
      word => {
        acc.add(word)
      }
    )
    //4.获取值

    val WD= acc.value
    println(WD)


    sc.stop()

  }

  class MyAccumulator() extends AccumulatorV2[String, mutable.Map[String, Int]] {


    private val wcMap = mutable.Map[String, Int]()

    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {

      new MyAccumulator
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    override def add(v: String): Unit = {
      val oldCnt: Int = wcMap.getOrElse(v, 0)
      wcMap.update(v, oldCnt + 1)
    }

    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      other.value.foreach {
        case (word, otherCnt) => {
          wcMap.getOrElse(word, 0)
          wcMap.update(word, otherCnt + otherCnt)
        }
      }
    }

    override def value: mutable.Map[String, Int] = {
      wcMap
    }
  }
}
