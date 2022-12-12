package com.wk.data.spark.service.analysis.executor.impl;

import com.wk.data.spark.service.analysis.executor.ClusterService;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Value;
import scala.collection.Seq;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.wk.data.spark.infrastructure.util.General.LEFT_OUTER;
import static org.apache.spark.sql.functions.*;
import static scala.Predef.wrapRefArray;

/**
 * @program: data-spark-job
 * @description: 聚类分析
 * @author: gwl
 * @create: 2022-07-25 15:59
 **/
public class ClusterServiceImpl implements ClusterService {

    private static String url = "jdbc:mysql://139.9.75.19:3306/hxc_sc_pro";
    private static String driver = "com.mysql.cj.jdbc.Driver";
    private static String username = "safectl";
    private static String password = "123-Qwe.";
    private static Properties properties = new Properties();
    static {
        properties.put("user", username);
        properties.put("password", password);
        properties.put("driver", driver);
    }

    public static void main(String[] args) {
        readWriterTimeZone();
    }

    private static void readWriterTimeZone() {
        SparkSession session = SparkSession.builder()
                .config("date", LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE))
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = session.read().format("com.mongodb.spark.sql")
                    .option("uri", "mongodb://root:123456@10.50.125.142:27017/data-etl?authSource=admin&authMechanism=SCRAM-SHA-256")
                    .option("collection", "sys_data_dict")
                    .load();
        List<Column> list = Arrays.asList(col("gmtCreate"), col("gmtUpdate"), col("code"), col("value"));
        dataset = dataset.select((Seq) wrapRefArray(list.toArray()));
        dataset.printSchema();
        dataset.show();
        dataset.write()
                .mode(SaveMode.Overwrite)
                .format("jdbc")
                .option("url", "jdbc:mysql://10.50.125.141:3306/test?useSSL=false&&characterEncoding=utf8&serverTimezone=GMT%2B8")
                .option("dbTable", "dict")
                .option("user", "root")
                .option("truncate", true)
                .option("password", "hxc@069.root_mysql")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("batchSize", 10000)
                .option("isolationLevel", "NONE")
                .save();
    }

    private static void clusterAbnormal() {
        SparkSession sparkSession = SparkSession.builder()
                .config("date", LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE))
                .master("local[*]")
                .getOrCreate();

        // 设备故障报警数据
        Dataset<Row> oeaDf = sparkSession.read().jdbc(url, "operaror_equipment_abnormal", properties)
                .where("trigger_time > '2022-07-19 00:00:00'");
        // 设备温度数据
        Dataset<Row> oetDf = sparkSession.read().jdbc(url, "operaror_equipment_temperature", properties)
                .where("trigger_time > '2022-07-01 00:00:00' and temperature IS NOT NULL");
        oetDf.show();

        // 车辆数据
        Dataset<Row> carDf = sparkSession.read().jdbc(url, "base_car", properties);
        // 设备基础信息
        Dataset<Row> ebDf = sparkSession.read().jdbc(url, "operator_equipment", properties);
        // 设备参数详情信息
        Dataset<Row> eqaDf = sparkSession.read().jdbc(url, "operator_equipment_arguments", properties);

        // 设备基础信息、设备参数信息和车辆信息关联出 车辆设备信息
        Dataset<Row> baseDf = ebDf.join(eqaDf, ebDf.col("id").equalTo(eqaDf.col("equipment_id")), LEFT_OUTER)
                .join(carDf, eqaDf.col("car_license").equalTo(carDf.col("car_license")), LEFT_OUTER)
                .select(col("province"), col("city"), col("county"), ebDf.col("school_code"), ebDf.col("school_name"),
                        col("status"), ebDf.col("gmt_create"), datediff(current_date(), ebDf.col("gmt_create")).as("eqdays"), col("equipment_no"), col("equipment_sn"),
                        col("equipment_type"), col("sim_no"), col("manufacturer"), col("model"), col("imei"),
                        col("soft_version"), col("camera_work"), carDf.col("car_license"), col("plate_color"),
                        col("model_id"), datediff(current_date(), carDf.col("gmt_create")).as("cardays"));

        sparkSession.close();
    }

}
