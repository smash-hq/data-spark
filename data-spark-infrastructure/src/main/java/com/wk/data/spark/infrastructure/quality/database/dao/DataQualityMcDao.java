/*
package com.wk.data.spark.infrastructure.quality.database.dao;

import com.alibaba.fastjson.JSON;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.wk.data.spark.infrastructure.quality.database.po.DataQualityDO;
import com.wk.data.spark.infrastructure.quality.database.po.QualityDO;
import com.wk.data.spark.infrastructure.util.Object2DocUtil;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

*/
/**
 * @Author: smash_hq
 * @Date: 2021/9/27 10:08
 * @Description: dataQuality的mongoClient
 * @Version v1.0
 *//*

@Repository
public class DataQualityMcDao {
    @Autowired
    @Qualifier("dataEMongoClient")
    private MongoClient dataEtlMongoClient;

    public void dataQualityUpdate(String ruleSerial, DataQualityDO dataQualityDO) {
        MongoCollection<Document> qualityCollection = collection();
        Document filter = new Document();
        Document document = new Document();
        filter.append("ruleSerial", ruleSerial);
        Document doc = Object2DocUtil.toDoc(dataQualityDO);
        document.put("$set", doc);
        qualityCollection.updateOne(filter, document);
    }

    public List<QualityDO> getQualityDos(String ruleSerial) {
        MongoCollection<Document> qualityCollection = collection();
        Document filter = new Document();
        Document document = new Document();
        filter.append("ruleSerial", ruleSerial);
        FindIterable<Document> documents = qualityCollection.find(filter);
        BsonDocument bsonDocument = Objects.requireNonNull(documents.first()).toBsonDocument(getClass(), qualityCollection.getCodecRegistry());
        BsonValue value = bsonDocument.get("qualityDOS");
        List<QualityDO> qualityDOList = new ArrayList<>();
        value.asArray().getValues().forEach(bsonValue -> {
            QualityDO qualityDO = JSON.parseObject(String.valueOf(bsonValue), QualityDO.class);
            qualityDOList.add(qualityDO);
        });
        return qualityDOList;
    }


    private MongoCollection<Document> collection() {
        CodecRegistry pojoCodecRegistry = org.bson.codecs.configuration.CodecRegistries.fromRegistries(
                MongoClientSettings.getDefaultCodecRegistry(),
                org.bson.codecs.configuration.CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        return dataEtlMongoClient.getDatabase("data-etl").getCollection("quality_data_cfg").withCodecRegistry(com.mongodb.MongoClient.getDefaultCodecRegistry());
    }
}
*/
