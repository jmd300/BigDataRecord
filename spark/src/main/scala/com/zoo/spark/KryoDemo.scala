package com.zoo.spark

import org.apache.spark.SparkConf
import utils.Function.createLocalSparkSessionAndSparkContext

import scala.collection.immutable.HashMap

/**
 * @Author: JMD
 * @Date: 4/23/2023
 */
object KryoDemo {
  private class MyClass {
    val name = "MyClassName"
  }
  def main(args: Array[String]): Unit = {
    val (spark, sc) = createLocalSparkSessionAndSparkContext()
    import spark.implicits._

    //向Kryo Serializer注册类型
    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getName)
    conf.registerKryoClasses(Array(
      classOf[Array[String]],
      classOf[HashMap[String, String]],
      classOf[MyClass]
    ))
  }

}

