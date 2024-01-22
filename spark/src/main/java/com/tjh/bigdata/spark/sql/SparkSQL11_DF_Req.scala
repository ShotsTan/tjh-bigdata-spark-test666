package com.tjh.bigdata.spark.sql

import org.apache.spark.sql._


/**
 * @ClassName: SparkSQL01_DF
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/09 22:42
 * @Company: Copyright©
 * */
object SparkSQL11_DF_Req {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "tjh")

    //获取环境对象
    val spark = new SparkSession.Builder().
      master("local[*]").
      appName("SparkSQL_Req").
      enableHiveSupport().
      getOrCreate()

    ///data/spark/data/sparksql/  city_info.txt  product_info.txt 	user_visit_action.txt

    spark.sql("use spark_01")

    spark.sql(
      """
        |select *
        |from (select *,
        |             rank() over (partition by area order by cnt DESC ) as `rank`
        |      from (select area,
        |                   product_name,
        |                   count(1) as cnt
        |            from (select ac.*,
        |                         city_name,
        |                         area,
        |                         product_name
        |                  from user_visit_action ac
        |                           join city_info ci
        |                                on ac.city_id = ci.city_id
        |                           join product_info pi
        |                                on product_id = ac.click_product_id
        |                  where click_product_id != -1) t2
        |            group by area, product_name)) t3
        |where rank <= 3 order by area;
        |""".stripMargin).show()


    spark.stop()
  }


}
