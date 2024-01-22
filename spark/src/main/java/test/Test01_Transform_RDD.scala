package test

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @ClassName: Test_Transform_RDD
 * @Description: TODO
 * @Author: Tanjh
 * @Date: 2023/01/06 9:52
 * @Company: Copyright©
 * */
object Test01_Transform_RDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark_transform_RDD")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.textFile("data/agent.log")
    //时间戳 省份 城市 用户 广告
    //每个省份每个广告被点击次数top3
//    val rdd2: RDD[(String, Iterable[String])] = rdd1.groupBy(x => x.split(" ")(1))
//    rdd2.map{
//      case (prv,line)=> {
//        val adv: Iterable[String] = line.map(x => x.split(" ")(4))
//        val map: List[(String, Int)] = adv.groupBy(x => x).map(x => (x._1, x._2.size)).toList.sortBy(x => x._2)(Ordering.Int.reverse).take(3)
//        (prv,map)
//      }
//    }.collect().foreach(println)

    rdd1.map(x=>((x.split(" ")(1),x.split(" ")(4)),1)).
      reduceByKey(_+_).
      map(x=>(x._1._1,(x._1._2,x._2))).
      groupBy(x=>x._1).
      map(x=> (x._1,x._2.toList.sortBy(x=>x._2._2)(Ordering.Int.reverse).take(3))).collect().foreach(println)

    sc.stop()

  }

}
