//package com.wk.data.demo;
//
//import com.wk.data.spark.service.demo.executor.SparkDemoService;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.CommandLineRunner;
//import org.springframework.boot.SpringApplication;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//
///**
// * @author
// * @Classname Application
// * @Description Spring Boot Starter
// * @Date 2021/5/27 3:44 下午
// * @Created by fengwei.cfw
// */
//@SpringBootApplication(scanBasePackages = {"com.wk.data.spark"})
//public class SparkDemoApplication implements CommandLineRunner {
//
//    @Autowired
//    private SparkDemoService sparkDemoService;
//
//    public static void main(String[] args) {
//        SpringApplication.run(SparkDemoApplication.class, args);
//    }
//
//    @Override
//    public void run(String... args) throws Exception {
//        sparkDemoService.wordCountTest(args);
//    }
//}
