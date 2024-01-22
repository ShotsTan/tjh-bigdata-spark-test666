package com.tjh.bigdata.spark.sql

import org.apache.spark.sql._


/**
 * @ClassName: SparkSQL01_DF
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/09 22:42
 * @Company: Copyright©
 * */
object SparkSQL10_DF_Req {
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
        |CREATE TABLE if not exists user_visit_action(
        |date string,
        |user_id bigint,
        |session_id string,
        |page_id bigint,
        |action_time string,
        |search_keyword string,
        |click_category_id bigint,
        |click_product_id bigint,
        |order_category_ids string,
        |order_product_ids string,
        |pay_category_ids string,
        |pay_product_ids string,
        |city_id bigint)
        |row format delimited fields terminated by '\t';
        |""".stripMargin)
    spark.sql(
      """
        |load data inpath 'hdfs://tanjh01:8020/data/spark/data/sparksql/user_visit_action.txt' into table user_visit_action;
        |""".stripMargin)

    spark.sql(
      """
        |
        |CREATE TABLE if not exists product_info(
        |product_id bigint,
        |product_name string,
        |extend_info string)
        |row format delimited fields terminated by '\t';
        |""".stripMargin)
    spark.sql(
      """
        |load data inpath 'hdfs://tanjh01:8020/data/spark/data/sparksql/product_info.txt' into table product_info;
        |""".stripMargin)
    spark.sql(
      """
        |
        |CREATE TABLE if not exists city_info(
        |city_id bigint,
        |city_name string,
        |area string)
        |row format delimited fields terminated by '\t';
        |""".stripMargin)
    spark.sql(
      """
        |
        |load data inpath 'hdfs://tanjh01:8020/data/spark/data/sparksql/city_info.txt' into table city_info;
        |""".stripMargin)

    spark.sql("show tables").show()


    spark.stop()
  }


}
