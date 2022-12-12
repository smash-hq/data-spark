//package com.wk.data.spark.service.demo.executor;
//
//
//import org.apache.spark.sql.sources.v2.DataSourceOptions;
//import org.apache.spark.sql.sources.v2.DataSourceV2;
//import org.apache.spark.sql.sources.v2.ReadSupport;
//import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.MetadataBuilder;
//import org.apache.spark.sql.types.StructField;
//import org.apache.spark.sql.types.StructType;
//
//import java.util.Optional;
//
///**
// * @Author: smash_hq
// * @Date: 2022/5/24 16:37
// * @Description:
// * @Version v1.0
// */
//public class ShardingsphereJdbc implements DataSourceV2, ReadSupport {
//    /**
//     * 指定数据源StructType，后续可以根据实际情况进行动态生成
//     */
//    public static final StructType SCHEMA = new StructType(
//            new StructField[]{
//                    new StructField("id", DataTypes.IntegerType, false, new MetadataBuilder().build()),
//                    new StructField("age", DataTypes.IntegerType, false, new MetadataBuilder().build())
//            }
//    );
//
//    @Override
//    public DataSourceReader createReader(DataSourceOptions dataSourceOptions) {
//        Optional<String> db = dataSourceOptions.get("db");
//        Optional<String> table = dataSourceOptions.get("table");
//        return new ExampleDataSourceReader(db, table);
//    }
//}
//
//
