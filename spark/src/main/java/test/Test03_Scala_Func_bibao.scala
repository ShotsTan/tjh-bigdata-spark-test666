package test

/**
 * @ClassName: Test02_Scala_Func
 * @Description: TODO 类描述 
 * @Author: Tanjh
 * @Date: 2023/01/06 10:00
 * @Company: Copyright©
 * */
object Test03_Scala_Func_bibao {
  def main(args: Array[String]): Unit = {
    def outer() = {
      val a = 100

      def inner(): Unit = {
        val b = 200
        println(a + b)
      }

      inner _
    }

    val funObj= outer()

    funObj()


    println()
  }

}
