//package com.wk.data.spark.infrastructure.quality.database.dao;
//
//import com.wk.data.spark.infrastructure.mongodb.database.MongoDbDao;
//import com.wk.data.spark.infrastructure.quality.database.po.ReportDO;
//import org.springframework.data.mongodb.core.query.Criteria;
//import org.springframework.data.mongodb.core.query.Query;
//import org.springframework.stereotype.Repository;
//
///**
// * @Author: smash_hq
// * @Date: 2021/9/1 17:21
// * @Description: 质量报告详情操作
// * @Version v1.0
// */
//@Repository
//public class ReportDao extends MongoDbDao<ReportDO> {
//    @Override
//    protected Class<ReportDO> getEntityClass() {
//        return ReportDO.class;
//    }
//
//    public String getReportId(String detectionCode) {
//        Query query = new Query();
//        query.addCriteria(Criteria.where("detectionTable").is(detectionCode));
//        return getOne(query).getId();
//    }
//
//    public void insertOne(ReportDO reportDO) {
//        insert(reportDO);
//    }
//}
