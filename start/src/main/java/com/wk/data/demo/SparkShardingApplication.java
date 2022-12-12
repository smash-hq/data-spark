//package com.wk.data.demo;
//
//import com.wk.data.spark.service.demo.executor.SparkShardingHandleService;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.CommandLineRunner;
//import org.springframework.boot.SpringApplication;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//import org.springframework.cloud.openfeign.EnableFeignClients;
//
///**
// * @Author: smash_hq
// * @Date: 2022/5/24 10:05
// * @Description: 启动spark分区读写
// * @Version v1.0
// */
//@SpringBootApplication(scanBasePackages = {"com.wk.data.spark"})
//@EnableFeignClients(basePackages = {"com.wk.data.etl.facade", "com.wk.data.share.facade"})
//public class SparkShardingApplication implements CommandLineRunner {
//
//    @Autowired
//    private SparkShardingHandleService service;
//
//    public static void main(String[] args) {
//        try {
//            System.setProperty("nacos.logging.default.config.enabled", "false");
//            SpringApplication.run(SparkShardingApplication.class, args);
//        } finally {
//            System.exit(0);
//        }
//
//    }
//
//    @Override
//    public void run(String[] args) {
//        service.shardingLoad(args);
//    }
//}
