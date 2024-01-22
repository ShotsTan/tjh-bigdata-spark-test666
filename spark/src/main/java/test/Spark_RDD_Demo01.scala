package test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_RDD_Demo
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/10 15:44
 * @Company: Copyright©
 * */
object Spark_RDD_Demo01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark_RDD_Demo01")
    val sc = new SparkContext(conf)

    val rdd0: RDD[Array[String]] = sc.textFile("data/user_visit_action.txt").map(x => x.split("_"))
    rdd0.cache()

    rdd0.collect().foreach(x=>println(x.mkString(",")))


    sc.stop()
  }


}
