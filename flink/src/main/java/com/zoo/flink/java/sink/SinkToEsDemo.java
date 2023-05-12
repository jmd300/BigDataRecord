package com.zoo.flink.java.sink;

import com.zoo.flink.java.FlinkEnv;
import com.zoo.flink.java.pojo.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @Author: JMD
 * @Date: 5/11/2023
 *
 * 并不用在es中先创建索引就能成功
 */
public class SinkToEsDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        ArrayList<HttpHost> httpHosts = new ArrayList<>();

        httpHosts.add(new HttpHost("hadoop102", 9200, "http"));

        // 创建一个 ElasticsearchSinkFunction
        ElasticsearchSinkFunction<Event> elasticsearchSinkFunction = new
                ElasticsearchSinkFunction<Event>() {
                    @Override
                    public void process(Event element, RuntimeContext ctx, RequestIndexer indexer) {
                        HashMap<String, String> data = new HashMap<>();
                        data.put(element.user, element.url);
                        IndexRequest request = Requests.indexRequest()
                                .index("clicks")
                                // .type("type") // Es 6 必须定义 type
                                .source(data);
                        indexer.add(request);
                    }
                };

        arrayStream.addSink(new ElasticsearchSink.Builder<Event>(httpHosts, elasticsearchSinkFunction).build());

        env.execute();
    }
}
