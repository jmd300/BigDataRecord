package com.zoo.flink.scala.source

import com.zoo.flink.java.pojo.Event
import com.zoo.flink.scala.FlinkEnv
import org.apache.flink.streaming.api.scala.createTypeInformation

/**
 * Author: JMD
 * Date: 5/12/2023
 */
object SourceDemo extends FlinkEnv{
  def main(args: Array[String]): Unit = {
    val clicks = Array[Event](new Event("Mary", "./home", 1000L), new Event("Bob", "./cart", 2000L))

    // 从集合中获取DataStream// 从集合中获取DataStream
    val stream1 = env.fromCollection(clicks)

    val stream2 = env.fromElements(
      new Event("Mary", "./home", 1000L),
      new Event("Bob", "./cart", 2000L))

    stream1.print
    stream2.print

    /**
     * 参数可以是目录，也可以是文件；
     * 路径可以是相对路径，也可以是绝对路径；
     * 相对路径是从系统属性 user.dir 获取路径: idea 下是 project 的根目录, standalone 模式下是集群节点根目录；
     * 也可以从 hdfs 目录下读取, 使用路径 hdfs://..., 由于 Flink 没有提供 hadoop 相关依赖,
     * 需要 pom 中添加相关依赖
     *//**
     * 参数可以是目录，也可以是文件；
     * 路径可以是相对路径，也可以是绝对路径；
     * 相对路径是从系统属性 user.dir 获取路径: idea 下是 project 的根目录, standalone 模式下是集群节点根目录；
     * 也可以从 hdfs 目录下读取, 使用路径 hdfs://..., 由于 Flink 没有提供 hadoop 相关依赖,
     * 需要 pom 中添加相关依赖
     */
    val stream3 = env.readTextFile("input/words.txt")
    stream3.print

    env.execute()
  }
}
