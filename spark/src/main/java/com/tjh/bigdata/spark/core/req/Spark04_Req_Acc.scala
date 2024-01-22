package com.tjh.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @ClassName: Spark01_Req
 * @Description: TODO Spark Core 案例
 * @Author: Tanjh
 * @Date: 2023/01/07 21:37
 * @Company: Copyright©
 * */
object Spark04_Req_Acc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req")
    val sc = new SparkContext(config = conf)

    // TODO 1.读取原始数据
    val rdd0: RDD[String] = sc.textFile("data/user_visit_action.txt")
    //    rdd0.persist()

    val acc = new HotCategoryAcc()

    sc.register(acc)

    // TODO 2.使用累加器统计分析
    //  2.1 统计点击数量
    rdd0.foreach(
      data => {
        val datas: Array[String] = data.split("_")
        if (datas(6) != "-1") {
          acc.add((datas(6), "click"))
        } else if (datas(8) != "null") {
          datas(8).split(",").foreach(
            id => acc.add((id, "order"))
          )
        } else if (datas(10) != "null") {
          datas(10).split(",").foreach(
            id => acc.add((id, "pay"))
          )
        }

      }
    )
    val accMap: mutable.Map[String, HotCategoryCount] = acc.value
    accMap.map(_._2).toList.sortWith(
      (c2,c1) => {
        if( c1.clickCnt >c2.clickCnt){
          true
        }else if(c1.clickCnt == c2.clickCnt){
          if(c1.orderCnt >c2.orderCnt){
            true
          }else if(c1.payCnt >c2.payCnt){
            c1.payCnt > c2.payCnt
          }else{
            false
          }
        }
        else{
          false
        }
      }
    ).take(10).foreach(println)


    // TODO 3.对数据排序，取前10名
    sc.stop()

  }

  case class HotCategoryCount(var cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int) {

  }

  class HotCategoryAcc extends AccumulatorV2[(String, String), mutable.Map[String, HotCategoryCount]] {
    override def isZero: Boolean = hcMap.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategoryCount]] = {

      new HotCategoryAcc()
    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      val (cid, actionType) = v
      val cnt: HotCategoryCount = hcMap.getOrElse(cid, HotCategoryCount(cid, 0, 0, 0))
      actionType match {
        case "click" => cnt.clickCnt += 1
        case "order" => cnt.orderCnt += 1
        case "pay" => cnt.payCnt += 1
      }
      hcMap.update(cid, cnt)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategoryCount]]): Unit = {
      other.value.foreach {
        case (cid, otherCnt) => {
          val thisCnt: HotCategoryCount = hcMap.getOrElse(cid, HotCategoryCount(cid, 0, 0, 0))
          thisCnt.clickCnt += otherCnt.clickCnt
          thisCnt.orderCnt += otherCnt.orderCnt
          thisCnt.payCnt += otherCnt.payCnt

          hcMap.update(cid, thisCnt)
        }
      }

    }

    override def value: mutable.Map[String, HotCategoryCount] = {
      hcMap
    }

    private val hcMap = mutable.Map[String, HotCategoryCount]()
  }

}
