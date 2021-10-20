package com.example.spark_cloud.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class ESUtils {
    private RestHighLevelClient client;

    public ESUtils() {
        if (this.client == null) {
            this.client = new RestHighLevelClient(RestClient.builder(new HttpHost[]{new HttpHost("10.133.69.110", 9200, "http")}).setHttpClientConfigCallback(new HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "SVRPNMLd17VChFM4bIk1"));
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            }));
        }

    }

    public void shutdown() {
        if (this.client != null) {
            try {
                this.client.close();
            } catch (IOException var2) {
                var2.printStackTrace();
            }
        }

    }

    public Map getById(String index, String id) {
        GetRequest getRequest = new GetRequest(index, id);
        GetResponse response = null;

        try {
            response = this.client.get(getRequest, RequestOptions.DEFAULT);
        } catch (IOException var6) {
            var6.printStackTrace();
        }

        Map<String, Object> source = response.getSource();
        return source;
    }

    public SearchHits search(String index, int page, int size, String key, String value) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        if (key != null && value != null && !key.equals("") && !value.equals("")) {
            QueryBuilder matchQueryBuilder = QueryBuilders.matchPhraseQuery(key, value);
            sourceBuilder.query(matchQueryBuilder);
        }

        sourceBuilder.from(page);
        sourceBuilder.size(size);
        sourceBuilder.timeout(new TimeValue(60L, TimeUnit.SECONDS));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(new String[]{index});
        searchRequest.source(sourceBuilder);
        List<Map<String, Object>> list = new ArrayList();
        SearchHits searchHits = null;

        try {
            SearchResponse searchResponse = this.client.search(searchRequest, RequestOptions.DEFAULT);
            searchHits = searchResponse.getHits();
            SearchHit[] var11 = searchHits.getHits();
            int var12 = var11.length;

            for(int var13 = 0; var13 < var12; ++var13) {
                SearchHit hit = var11[var13];
                System.out.println(hit.getSourceAsString());
                list.add(hit.getSourceAsMap());
            }
        } catch (IOException var15) {
            var15.printStackTrace();
        }

        return searchHits;
    }

    private void deleteCommon(String index, SearchHits searchHits) {
        new DeleteRequest();
        SearchHit[] var4 = searchHits.getHits();
        int var5 = var4.length;

        for(int var6 = 0; var6 < var5; ++var6) {
            SearchHit hit = var4[var6];
            DeleteRequest deleteRequest = new DeleteRequest(index, hit.getId());

            try {
                DeleteResponse deleteResponse = this.client.delete(deleteRequest, RequestOptions.DEFAULT);
                System.out.println("Delete Done【" + deleteResponse.getId() + "】,Status:【" + deleteResponse.status() + "】");
            } catch (IOException var9) {
                var9.printStackTrace();
            }
        }

    }

    private void deleteById(String index, String id) {
        DeleteRequest deleteRequest = new DeleteRequest(index, id);

        try {
            DeleteResponse deleteResponse = this.client.delete(deleteRequest, RequestOptions.DEFAULT);
            System.out.println("Delete Done【" + deleteResponse.getId() + "】,Status:【" + deleteResponse.status() + "】");
        } catch (IOException var5) {
            var5.printStackTrace();
        }

    }

    private void deleteAsync(String index, SearchHits searchHits) {
        new DeleteRequest();
        SearchHit[] var4 = searchHits.getHits();
        int var5 = var4.length;

        for(int var6 = 0; var6 < var5; ++var6) {
            final SearchHit hit = var4[var6];
            DeleteRequest deleteRequest = new DeleteRequest(index, hit.getId());
            this.client.deleteAsync(deleteRequest, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>() {
                @Override
                public void onResponse(DeleteResponse deleteResponse) {
                    RestStatus restStatus = deleteResponse.status();
                    int status = restStatus.getStatus();
                    deleteResponse.getId();
                    System.out.println("Delete Done【" + deleteResponse.getId() + "】,Status:【" + status + "】");
                }

                public void onFailure(Exception e) {
                    e.printStackTrace();
                    System.out.println("ERROR  " + hit.getId());
                }
            });
        }

    }

    public static void main(String[] args) throws Exception {
        String index = "calculation";
        String type = "flink-type";
        ESUtils es = new ESUtils();
        SearchHits shs = es.search(index, 1, 100, (String)null, (String)null);
       // es.deleteById(index, "3j3bFnkBwpCNdBmX6K6f");
        System.out.println(shs);
    }
}
