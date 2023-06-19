package com.zoo.flink.scala.sink

import com.zoo.flink.java.util.Event
import com.zoo.flink.scala.util.FlinkEnv
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}

import java.sql.PreparedStatement

/**
 * Author: JMD
 * Date: 5/12/2023
 */
object SinkToMysqlDemo extends FlinkEnv{
  def main(args: Array[String]): Unit = {
    val jdbcStatementBuilder: JdbcStatementBuilder[Event] = new JdbcStatementBuilder[Event](){
      override def accept(ps: PreparedStatement, event: Event): Unit = {
        ps.setString(1, event.user)
        ps.setString(2, event.url)
      }
    }

    // 用函数方式传递 jdbcStatementBuilder就报错类型不匹配，没能解决，现在只可以用匿名类
    arrayStream.addSink(
      JdbcSink.sink(
        s"INSERT INTO clicks (user, url) VALUES (?, ?)",
/*        (ps: PreparedStatement, event: Event) => {
          ps.setString(1, event.user)
          ps.setString(2, event.url)
        }.asInstanceOf[JdbcStatementBuilder[Event]],*/
        jdbcStatementBuilder,
        JdbcExecutionOptions.builder()
          .withBatchSize(1000)
          .withBatchIntervalMs(200)
          .withMaxRetries(5)
          .build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl("jdbc:mysql://hadoop102:3306/data")
          .withDriverName("com.mysql.cj.jdbc.Driver")
          .withUsername("root")
          .withPassword("1234")
          .build()
      ))

    env.execute
  }
}
