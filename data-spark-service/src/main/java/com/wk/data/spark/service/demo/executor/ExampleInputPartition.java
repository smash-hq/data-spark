//package com.wk.data.spark.service.demo.executor;
//
//import lombok.SneakyThrows;
//import org.apache.spark.sql.sources.v2.reader.InputPartition;
//import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
//
//import java.io.Serializable;
//
///**
// * @author smash_hq
// */
//public class ExampleInputPartition implements InputPartition, Serializable {
//
//    public static String db;
//    public static String table;
//
//    public ExampleInputPartition(String dbCode, String dbTable) {
//        db = String.valueOf(dbCode);
//        table = String.valueOf(dbTable);
//    }
//
//    @SneakyThrows
//    @Override
//    public InputPartitionReader createPartitionReader() {
//        return new ExamplePartitionReader(db,table);
//    }
//}
