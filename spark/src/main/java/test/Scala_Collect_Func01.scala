package test

/**
 * @ClassName: Scala_Collect_Func
 * @Description: TODO 集合常用函数
 * @Author: Tanjh
 * @Date: 2023/01/09 11:16
 * @Company: Copyright©
 * */
object Scala_Collect_Func01 {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val map = Map(("a", 1), ("b", 2), ("c", 3), ("d", 5))

    // TODO 1.集合常用函数
    //    （1）获取集合长度
    println(list.length)
    println("**" * 15)

    //    （2）获取集合大小
    println(list.size)
    println(map.size)
    println("**" * 15)

    //    （3）循环遍历
    list.foreach(println)

    println("**" * 15)

    //    （4）迭代器
    for (elem <- list.iterator){
      println(elem)
    }
    println("**" * 15)

    //    （5）生成字符串
    println(list.mkString("[", ",", "]"))
    println(map.mkString("[", ",", "]"))
    println("**" * 15)

    //    （6）是否包含
    println(list.contains(3))
    println(map.contains("a"))

  }

}
