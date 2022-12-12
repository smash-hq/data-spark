//package com.wk.data.spark.service.demo.executor.impl;
//
//import com.wk.data.spark.infrastructure.config.ShardingsphereDatasource;
//import com.wk.data.spark.service.demo.executor.SparkShardingHandleService;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Component;
//
///**
// * @Author: smash_hq
// * @Date: 2022/5/24 9:49
// * @Description:
// * @Version v1.0
// */
//@Component
//public class SparkShardingHandleServiceImpl implements SparkShardingHandleService {
//
//    public static final Logger LOGGER = LoggerFactory.getLogger(SparkShardingHandleServiceImpl.class);
//
//    @Autowired
//    private SparkSession sparkSession;
//
//    @Autowired
//    private ShardingsphereDatasource datasource;
//
//    @Override
//    public void shardingLoad(String[] args) {
//        mysqlRead(args[0], args[1]);
//    }
//
//    private void mysqlRead(String db, String tableCode) {
//        Dataset<Row> dataset = sparkSession.read()
//                .format("com.wk.data.spark.service.demo.executor.ShardingsphereJdbc")
//                .option("db", "db")
//                .option("table", "shardingsphere")
//                .load();
//        dataset.show();
//    }
//}
