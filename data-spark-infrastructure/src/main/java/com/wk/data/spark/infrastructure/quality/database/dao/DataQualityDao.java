//package com.wk.data.spark.infrastructure.quality.database.dao;
//
//import com.wk.data.spark.infrastructure.mongodb.database.MongoDbDao;
//import com.wk.data.spark.infrastructure.quality.database.po.DataQualityDO;
//import org.springframework.data.mongodb.core.query.Criteria;
//import org.springframework.data.mongodb.core.query.Query;
//import org.springframework.data.mongodb.core.query.Update;
//import org.springframework.stereotype.Repository;
//
///**
// * @Author: smash_hq
// * @Date: 2021/9/1 16:12
// * @Description: 保存质量规则
// * @Version v1.0
// */
//@Repository
//public class DataQualityDao extends MongoDbDao<DataQualityDO> {
//
//    @Override
//    protected Class<DataQualityDO> getEntityClass() {
//        return DataQualityDO.class;
//    }
//
//    /**
//     * 更新操作
//     *
//     * @param dataQualityDO
//     */
//    public void updateOne(DataQualityDO dataQualityDO) {
//        Query query = new Query();
//        query.addCriteria(Criteria.where("_id").is(dataQualityDO.getId()));
//        Update update = new Update();
//        update.set("reportId", dataQualityDO.getReportId());
//        update.set("problemTotal", dataQualityDO.getProblemTotal());
//        update.set("detectionTotal", dataQualityDO.getDetectionTotal());
//        update.set("updateTime", dataQualityDO.getUpdateTime());
//        updateFirst(query, update);
//    }
//}
