//package com.wk.data.spark.service.demo.executor.impl;
//
//import com.wk.data.spark.service.demo.executor.SparkDemoService;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.sql.SparkSession;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Component;
//
//import java.util.Arrays;
//import java.util.List;
//
///**
// * @program: data-spark-job
// * @description:
// * @author: gwl
// * @create: 2021-08-25 18:16
// **/
//@Component
//public class SparkDemoServiceImpl implements SparkDemoService {
//
//    @Autowired
//    private SparkSession sparkSession;
//
//    @Override
//    public void wordCountTest(String... args) {
//        JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
//
//        // 构造数据源
//        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
//
//        //并行化创建rdd
//        JavaRDD<Integer> rdd = sc.parallelize(data);
//
//        //map && reduce
//        Integer result = rdd.map((Function<Integer, Integer>) integer -> integer)
//                .reduce((Function2<Integer, Integer, Integer>) (o, o2) -> o + o2);
//
//        sc.close();
//        sparkSession.close();
//        System.out.println("执行结果：" + result);
//
//    }
//}
