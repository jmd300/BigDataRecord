package com.zoo.lang.es.poetry.search.rw;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zoo.lang.es.poetry.search.OracleConf;
import com.zoo.lang.es.poetry.search.Poetry;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Author: JMD
 * @Date: 3/15/2023
 */
public class WriteToEs {
    public static void main(String[] args) {
        try {
            // 建立数据库连接
            Connection conn = OracleConf.getConnection();
            // 执行查询
            PreparedStatement ps = conn.prepareStatement("select ID, AUTHOR, TOPIC, VERSE, \"SECTION\", NOTE  from poetry");
            ResultSet rs = ps.executeQuery();

            // 创建 es 客户端对象
            RestHighLevelClient client = new RestHighLevelClient(
                    RestClient.builder(new HttpHost("localhost", 9200, "http"))
            );

            // 新增文档 - 请求对象
            IndexRequest indexRequest = new IndexRequest();
            // 设置索引及唯一性标识

            ObjectMapper objectMapper = new ObjectMapper();
            int i = 0;

            // 处理查询结果
            while (rs.next()) {
                indexRequest.index("poetry-index").id(rs.getString("ID"));

                Poetry poetry = new Poetry();
                poetry.setId(rs.getString("ID"));
                poetry.setAuthor(rs.getString("AUTHOR"));
                poetry.setTitle(rs.getString("TOPIC"));
                poetry.setVerse(rs.getString("VERSE"));
                poetry.setSection(rs.getString("\"SECTION\""));
                poetry.setNote(rs.getString("NOTE"));
                // System.out.println("===" + poetry);

                String productJson = objectMapper.writeValueAsString(poetry);
                // 添加文档数据，数据格式为 JSON 格式
                indexRequest.source(productJson, XContentType.JSON);
                // 客户端发送请求，获取响应对象
                IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
                System.out.println("第" + i++ + " " + response);
            }
            System.out.println("over1");
            // 关闭查询结果和语句对象
            rs.close();
            System.out.println("rs.isClosed(): " + rs.isClosed());
            ps.close();
            System.out.println("over2");

            conn.close();
            client.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("over over");
    }
}