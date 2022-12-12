//package com.wk.data.spark.service.demo.executor;
//
//import org.apache.spark.sql.catalyst.InternalRow;
//import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
//import org.apache.spark.sql.sources.v2.reader.InputPartition;
//import org.apache.spark.sql.types.StructType;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Optional;
//
///**
// * @author smash_hq
// */
//public class ExampleDataSourceReader implements DataSourceReader {
//
//    public static String dbCode;
//    public static String dbTable;
//
//    public ExampleDataSourceReader(Optional<String> db, Optional<String> table) {
//        dbCode = db.get();
//        dbTable = table.get();
//    }
//
//
//    @Override
//    public StructType readSchema() {
//        return ShardingsphereJdbc.SCHEMA;
//    }
//
//    @Override
//    public List<InputPartition<InternalRow>> planInputPartitions() {
//        List<InputPartition<InternalRow>> partitions = new ArrayList();
//
//        // 全局唯一的数据分区
//        partitions.add(new ExampleInputPartition(dbCode, dbTable));
//        return partitions;
//    }
//}
