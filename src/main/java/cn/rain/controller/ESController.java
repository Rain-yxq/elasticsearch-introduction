package cn.rain.controller;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * description:
 *
 * @author 任伟
 * @date 2018/5/29 14:44
 */
@RestController
public class ESController {

    @Autowired
    private TransportClient client;

    /**
     * 根据id查询书籍信息
     */
    @GetMapping("/book/search")
    public String getBook(String id){
        GetResponse result = client.prepareGet("book", "novel", id).get();
        return result.getSource().toString();
    }

    /**
     * 新增书籍
     */
    @GetMapping("/book/insert")
    public String insertBook(
            @RequestParam(name = "title") String title,
            @RequestParam(name = "author") String author,
            @RequestParam(name = "word_count") int wordCount,
            @RequestParam(name = "publish_date")
                    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
                    Date publishDate) throws IOException {

        XContentBuilder contentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                    .field("title", title)
                    .field("author", author)
                    .field("word_count", wordCount)
                    .field("publish_date", publishDate.getTime())
                .endObject();

        IndexResponse response = client.prepareIndex("book", "novel").setSource(contentBuilder).get();
        return "新增图书的id为： " + response.getId();
    }

    /**
     * 删除书籍
     */
    @GetMapping("/book/delete")
    public String deleteBook(@RequestParam(name = "id") String id){
        DeleteResponse response = client.prepareDelete("book", "novel", id).get();
        return response.getResult().toString();
    }

    /**
     * 修改书籍
     */
    @GetMapping("/book/update")
    public String update(
            @RequestParam(name = "id") String id,
            @RequestParam(name = "title", required = false) String title,
            @RequestParam(name = "author", required = false) String author,
            @RequestParam(name = "word_count", required = false) Integer wordCount,
            @RequestParam(name = "publish_date", required = false) Date publishDate) throws IOException, ExecutionException, InterruptedException {

        UpdateRequest updateRequest = new UpdateRequest("book", "novel", id);

        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder().startObject();
        if (title != null){
            jsonBuilder.field("title", title);
        }
        if (author != null){
            jsonBuilder.field("author", author);
        }
        if (wordCount != null){
            jsonBuilder.field("word_count", wordCount);
        }
        if (publishDate != null){
            jsonBuilder.field("publish_date", publishDate);
        }
        jsonBuilder.endObject();
        updateRequest.doc(jsonBuilder);
        UpdateResponse response = client.update(updateRequest).get();
        return response.getResult().toString();
    }

    @GetMapping("/book/complexQuery")
    public List<Map<String, Object>> complexQuery(
            @RequestParam(name = "author", required = false) String author,
            @RequestParam(name = "title", required = false) String title,
            @RequestParam(name = "gt_word_count", defaultValue = "0") Integer gtWordCount,
            @RequestParam(name = "lt_word_count", required = false) Integer ltWordCount
    ){
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if (author != null){
            boolQuery.must(QueryBuilders.matchQuery("author", author));
        }
        if (title != null){
            boolQuery.must(QueryBuilders.matchQuery("title", title));
        }

        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("word_count");
        rangeQuery.from(gtWordCount);
        if (ltWordCount != null && ltWordCount > 0){
            rangeQuery.to(ltWordCount);
        }

        boolQuery.filter(rangeQuery);

        SearchRequestBuilder searchBuilder = client.prepareSearch("book")
                .setTypes("novel")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQuery)
                .setFrom(0)
                .setSize(10);

        // 这里可以输出查询的请求体
        System.out.println(searchBuilder);

        SearchResponse response = searchBuilder.get();
        List<Map<String, Object>> resultLst = new ArrayList<>();
        for (SearchHit hit : response.getHits()) {
            // 遍历到的每条数据
            Map<String, Object> source = hit.getSource();
            resultLst.add(source);
        }
        return resultLst;
    }
}
