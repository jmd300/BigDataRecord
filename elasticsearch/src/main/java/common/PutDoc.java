package common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * @Author: JMD
 * @Date: 3/15/2023
 */
@AllArgsConstructor
@Setter
@Getter
public class PutDoc {
    String addr = "localhost";
    int port = 9200;
    String schema = "http";

    IndexResponse put(String index, String id, String jsonDoc) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(addr, port, schema)));

        // 新增文档 - 请求对象
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(index).id(id);
        indexRequest.source(jsonDoc, XContentType.JSON);

        // 客户端发送请求，获取响应对象
        IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("_index:" + response.getIndex());
        System.out.println("_id:" + response.getId());
        System.out.println("_result:" + response.getResult());

        // 关闭 es 客户端连接
        client.close();
        return response;
    }
}

