package test

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @ClassName: Test06_SparkSQL_RDD2DF
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/13 9:18
 * @Company: Copyright©
 * */
object Test06_SparkSQL_RDD2DF {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = new SparkSession.Builder().master("local[*]").appName("SparkSQL_Demo").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)), 2)

    import spark.implicits._

    val ds: Dataset[User] = rdd1.map(x => User(x._1, x._2)).toDS()

    ds.select("name","id").where("id<3").show()
    //    rdd1.toDF("name", "id").createTempView("user")

    //    spark.sql("select max(id) from user").show()

    spark.read.json("data/user.json").show(false)


    sc.stop()
    spark.close()
  }

  case class User(name: String, id: Int)

}
