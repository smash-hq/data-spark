//package com.wk.data.spark.infrastructure.mongodb.database;
//
//
//import com.mongodb.client.result.UpdateResult;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Qualifier;
//import org.springframework.data.mongodb.core.MongoTemplate;
//import org.springframework.data.mongodb.core.aggregation.Aggregation;
//import org.springframework.data.mongodb.core.aggregation.AggregationResults;
//import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
//import org.springframework.data.mongodb.core.query.Criteria;
//import org.springframework.data.mongodb.core.query.Query;
//import org.springframework.data.mongodb.core.query.Update;
//
//import java.util.Collection;
//import java.util.List;
//
///**
// * @Author: smash_hq
// * @Date: 2021/8/31 16:20
// * @Description: 封装mongo
// * @Version v1.0
// */
//public abstract class MongoDbDao<T> {
//
//    /**
//     * 反射获取泛型类型
//     *
//     * @return
//     */
//    protected abstract Class<T> getEntityClass();
//
//    /**
//     * 数据库模板
//     */
//    @Autowired
//    @Qualifier("dataetlMongoTemplate")
//    protected MongoTemplate mongoTemplate;
//
//    /***
//     * 保存一个对象,若唯一性索引冲突，则直接覆盖
//     * @param t
//     * @return
//     */
//    public T save(T t) {
//        return this.mongoTemplate.save(t);
//    }
//
//    /**
//     * 保存一个对象,若唯一性索引冲突，则新增失败
//     *
//     * @param t
//     */
//    public T insert(T t) {
//        return (T) this.mongoTemplate.insert(t);
//    }
//
//    /***
//     * 保存对象列表
//     *
//     * @param batch
//     */
//    public Collection<T> batchInsert(Collection<T> batch) {
//        return this.mongoTemplate.insert(batch, this.getEntityClass());
//    }
//
//
//    /***
//     * 根据id从几何中查询对象
//     * @param id
//     * @return
//     */
//    public T getById(String id) {
//        Query query = new Query(Criteria.where("_id").is(id));
//        return this.mongoTemplate.findOne(query, this.getEntityClass());
//    }
//
//    /**
//     * 根据自定义条件查询集合
//     *
//     * @param query
//     * @return
//     */
//    public List<T> list(Query query) {
//        return mongoTemplate.find(query, this.getEntityClass());
//    }
//
//
//    /**
//     * 根据条件查询只返回一个文档
//     *
//     * @param query
//     * @return
//     */
//    public T getOne(Query query) {
//        return mongoTemplate.findOne(query.skip(0).limit(1), this.getEntityClass());
//    }
//
//    /***
//     * 根据条件分页查询
//     * @param query
//     * @param start 查询起始值
//     * @param size  查询大小
//     * @return
//     */
//    public List<T> getPage(Query query, int start, int size) {
//        return this.mongoTemplate.find(query.skip(start).limit(size), this.getEntityClass());
//    }
//
//    /***
//     * 根据条件查询库中符合条件的记录数量
//     * @param query
//     * @return
//     */
//    public int count(Query query) {
//        return (int) this.mongoTemplate.count(query, this.getEntityClass());
//    }
//
//    /**
//     * 根据自定义条件删除集合
//     *
//     * @param query
//     * @return
//     */
//    public int deleteBatch(Query query) {
//        return (int) mongoTemplate.remove(query, this.getEntityClass()).getDeletedCount();
//    }
//
//    /**
//     * 根据id删除
//     *
//     * @param id
//     */
//    public int deleteById(String id) {
//        Query query = new Query(Criteria.where("_id").is(id));
//        return (int) this.mongoTemplate.remove(query, this.getEntityClass()).getDeletedCount();
//    }
//
//    /**
//     * 修改匹配到的第一条记录
//     *
//     * @param query
//     * @param update
//     */
//    public UpdateResult updateFirst(Query query, Update update) {
//        return this.mongoTemplate.updateFirst(query, update, this.getEntityClass());
//    }
//
//    /**
//     * 修改匹配到的所有记录
//     *
//     * @param query
//     * @param update
//     */
//    public UpdateResult updateMulti(Query query, Update update) {
//        return this.mongoTemplate.updateMulti(query, update, this.getEntityClass());
//    }
//
//    /**
//     * 聚合查询统计
//     *
//     * @param
//     * @return
//     */
//    public <O> AggregationResults<O> aggregate(Aggregation aggregation, Class<O> outputType) {
//        return mongoTemplate.aggregate(aggregation, this.getEntityClass(), outputType);
//    }
//
//    /**
//     * 聚合查询统计
//     *
//     * @param
//     * @return
//     */
//    public <O> AggregationResults<O> aggregate(TypedAggregation aggregation, Class<O> outputType) {
//        return mongoTemplate.aggregate(aggregation, outputType);
//    }
//
//}
