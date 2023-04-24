package common;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * @Author: JMD
 * @Date: 3/15/2023
 */

public class CreateIndex {
    static XContentBuilder builder;
    static void addItem(String name, String type) throws IOException {
        builder.startObject(name);
        builder.field("type", type);
        builder.endObject();
    }

    public static void main(String[] args) throws IOException {
        // create the client
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));

        // create the index request
        CreateIndexRequest request = new CreateIndexRequest("poetry-index");

        // set the index settings
        request.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", "1s")
                .put("analysis.analyzer.default.type", "ik_smart")
        );

        // define the index mappings
        builder = XContentFactory.jsonBuilder();

        builder.startObject();
        {
            builder.startObject("poetry");
            {
                addItem("title1", "text");
                addItem("title2", "text");
                addItem("author", "text");
                addItem("verse", "text");
            }
            builder.endObject();
        }
        builder.endObject();
        request.mapping(builder.toString());

        // execute the request
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);

        // print the response
        System.out.println(response);

        // close the client
        client.close();
    }
}
