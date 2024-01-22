package test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

/**
 * @ClassName: Test04_Spark_UDF
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/11 16:35
 * @Company: Copyright©
 * */
object Test04_Spark_UDF {
  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder().master("local[*]").appName("SparkSQL_UDF_TEST").getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val rdd0 = sc.textFile("data/user_visit_action.txt").
      map(x => x.split("_")).
      map(x => (x(0), x(1).toLong, x(2), x(3).toLong, x(4), x(5), x(6).toLong,
        x(7).toLong, x(8), x(9), x(10), x(11), x(12).toLong))

    val rdd1: RDD[UserVisitAction] = rdd0.map {
      case (date, user_id, session_id, page_id, action_time,
      search_keyword, click_category_id, click_product_id,
      order_category_ids, order_product_ids, pay_category_ids,
      pay_product_ids, city_id)
      =>
        UserVisitAction(date, user_id, session_id, page_id, action_time,
          search_keyword, click_category_id, click_product_id, order_category_ids,
          order_product_ids, pay_category_ids, pay_product_ids, city_id)
    }
    //    rdd0.collect().foreach(x=>println(x.toList))
    val df: DataFrame = rdd1.toDF()
    df.show()

    df.createTempView("user")

    spark.udf.register("MaxID", functions.udaf(new MaxID()))

    spark.udf.register("sp", (ids: String) => {
      val strings: Array[String] = ids.split(",")
      if (strings.length > 0 && strings(0) != "null") {
        val ids: Array[Int] = strings.map((x: String) => x.toInt)
        ids.max.toString
      }else strings.mkString(",")
    })
    spark.sql("select sp(order_category_ids) from user").show()

    spark.sql("select MaxID(page_id) from user").show()


    spark.stop()
  }

  case class UserVisitAction(
                              date: String, //用户点击行为的日期
                              user_id: Long, //用户的 ID
                              session_id: String, //Session 的 ID
                              page_id: Long, //某个页面的 ID
                              action_time: String, //动作的时间点
                              search_keyword: String, //用户搜索的关键词
                              click_category_id: Long, //某一个商品品类的 ID
                              click_product_id: Long, //某一个商品的 ID
                              order_category_ids: String, //一次订单中所有品类的 ID 集合
                              order_product_ids: String, //一次订单中所有商品的 ID 集合
                              pay_category_ids: String, //一次支付中所有品类的 ID 集合
                              pay_product_ids: String, //一次支付中所有商品的 ID 集合
                              city_id: Long //城市 id
                            )
  //  case class IDBufferType(id:Int)

  class MaxID extends Aggregator[Long, Long, Long] {
    override def zero: Long = 0

    override def reduce(b: Long, a: Long): Long = {
      if (a >= b) a else b
    }

    override def merge(b1: Long, b2: Long): Long =
      if (b1 >= b2) b1 else b2

    override def finish(reduction: Long): Long = reduction

    override def bufferEncoder: Encoder[Long] = Encoders.scalaLong

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
