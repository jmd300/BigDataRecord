package com.zoo.flink.scala.source

import com.zoo.flink.scala.util.FlinkEnv
import org.apache.flink.api.scala.createTypeInformation

/**
 * Author: JMD
 * Date: 6/26/2023
 */
object SourceCustom extends FlinkEnv{
  def main(args: Array[String]): Unit = {
    //有了自定义的 source function，调用 addSource 方法//有了自定义的 source function，调用 addSource 方法

    val stream = env.addSource(new ClickSource())
    stream.print("SourceCustom")

    env.execute()
  }
}
