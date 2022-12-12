//package com.wk.data.spark.service.demo.executor.impl;
//
//import com.wk.data.spark.infrastructure.demo.dataset.DemoSparkSession;
//import com.wk.data.spark.infrastructure.util.cleaning.RegularRule;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.api.java.UDF2;
//import org.apache.spark.sql.functions;
//import org.apache.spark.sql.types.DataTypes;
//
///**
// * @Author: smash_hq
// * @Date: 2021/9/30 9:32
// * @Description: 测试dataset操作
// * @Version v1.0
// */
//
//public class DemoDatasetImpl {
//
//    public static Dataset<Row> arrayDemo(Dataset<Row> dataset) {
//        try {
////            dataset = dataset.agg(functions.array("new_line"));
//            dataset.select(functions.map(functions.col("name"))).show();
//        } catch (Exception e) {
//            System.out.println(e.getMessage() + ":" + e);
//        }
//        return dataset;
//    }
//
//    public static void main(String[] args) {
//        DemoSparkSession demoSparkSession = new DemoSparkSession();
//        SparkSession spark = demoSparkSession.demoSparkSession();
//        String pwd = "hxc@069.root_mysql";
//        String tableCode = "test_table";
//        Dataset<Row> dataset = spark.read().format("jdbc")
//                .option("url", "jdbc:mysql://10.50.125.141:3306/test_rule")
//                .option("driver", "com.mysql.cj.jdbc.Driver")
//                .option("user", "root")
//                .option("password", pwd)
////                .option("encoding","utf-8")
//                .option("query", "select * from " + tableCode)
//                .load();
//
//        Dataset<Row> arrayDataset = arrayDemo(dataset);
//
//        UDF2<String, String, Boolean> udf2 = new RegularRule<>();
//        spark.udf().register("udf2", udf2, DataTypes.BooleanType);
//
//        arrayDataset.show();
//        arrayDataset.printSchema();
//        spark.close();
//    }
//
//
//}
