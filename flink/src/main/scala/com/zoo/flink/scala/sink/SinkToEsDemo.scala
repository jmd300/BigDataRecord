package com.zoo.flink.scala.sink

import com.zoo.flink.java.util.Event
import com.zoo.flink.scala.util.FlinkEnv
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import java.util
import scala.collection.JavaConverters._

/**
 * Author: JMD
 * Date: 5/12/2023
 */
object SinkToEsDemo extends FlinkEnv{
  def main(args: Array[String]): Unit = {
    val httpHostArray = Array(new HttpHost("hadoop102", 9200, "http"))
    val httpHosts = bufferAsJavaList(httpHostArray.toBuffer)

    // 创建一个 ElasticsearchSinkFunction// 创建一个 ElasticsearchSinkFunction
    val elasticsearchSinkFunction: ElasticsearchSinkFunction[Event] = new ElasticsearchSinkFunction[Event]() {
      override def process(element: Event, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {

        val request: IndexRequest = Requests.indexRequest.index("clicks").source(Map((element.user, element.url)))
        indexer.add(request)
      }
    }

    arrayStream.addSink(new ElasticsearchSink.Builder[Event](httpHosts, elasticsearchSinkFunction).build)

    env.execute
  }
}
