/*
package com.wk.data.spark.infrastructure.quality.database.dao;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.wk.data.spark.infrastructure.quality.database.po.ReportDO;
import com.wk.data.spark.infrastructure.util.Object2DocUtil;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import java.util.Objects;

*/
/**
 * @Author: smash_hq
 * @Date: 2021/9/27 10:14
 * @Description: report的mongoClient
 * @Version v1.0
 *//*

@Repository
public class ReportMcDao {
    @Autowired
    @Qualifier("dataEMongoClient")
    private MongoClient dataEtlMongoClient;

    public void reportInsertOne(ReportDO reportDO) {
        MongoCollection<Document> reportCollection = collection();
        Document document = Object2DocUtil.toDoc(reportDO);
        reportCollection.insertOne(document);
    }

    public String getReportId(String detectionTable) {
        MongoCollection<Document> reportCollection = collection();
        FindIterable<Document> documents = reportCollection.find(new Document("detectionTable", detectionTable));
        return Objects.requireNonNull(documents.first()).get("_id").toString();
    }

    private MongoCollection<Document> collection() {
        return dataEtlMongoClient.getDatabase("data-etl").getCollection("quality_report");
    }

}
*/
