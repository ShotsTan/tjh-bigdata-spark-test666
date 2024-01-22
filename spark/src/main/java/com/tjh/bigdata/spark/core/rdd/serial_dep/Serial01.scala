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
object Serial01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Serial").set("spark.serializer",
      "org.apache.spark.serializer.KryoSerializer")
      // 注册需要使用 kryo 序列化的自定义类
//      .registerKryoClasses(Array(classOf[Searcher]))
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List("hello", "Hadoop", "Scala", "Spark"), 2)
    //    rdd1.filter(x => x(0) == 'S').collect().foreach(println)
    //    rdd1.filter(x => x.substring(0, 1) == "S").collect().foreach(println)

    new Search("S").matchRDD(rdd1)


    sc.stop()
  }

  //  case class User extends Serializable {
  class Search(query: String) {
    def matchRDD(rdd: RDD[String]): Unit = {
      val q = query
      rdd.filter(x => x.substring(0, 1) == q).collect().foreach(println)
    }
  }
}
