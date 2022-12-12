package com.wk.data.spark.service.govern.executor;

import com.alibaba.fastjson.JSON;
import com.wk.data.etl.facade.govern.dto.TaskNodeDTO;
import com.wk.data.etl.facade.govern.dto.split.DataSplitDTO;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static scala.Predef.wrapRefArray;

/**
 * @program: data-spark-job
 * @description: 数据拆分
 * @author: gwl
 * @create: 2021-12-10 15:33
 **/
public class DataSplitService extends Transform implements Serializable, scala.Serializable {

    private static Logger logger = LoggerFactory.getLogger(DataSplitService.class);

    private TaskNodeDTO node;

    private SparkSession session;


    public DataSplitService(TaskNodeDTO node, SparkSession sparkSession) {
        this.node = node;
        this.session = sparkSession;
    }

    @Override
    public void execute() throws Exception {
        List<String> dns = node.getDependNumber();
        String upTempView = dns.stream().filter(StringUtils::isNotBlank).findFirst().get();
        String tempView = node.getNumber();
        List<DataSplitDTO> rules = JSON.parseArray(JSON.toJSONString(node.getData()), DataSplitDTO.class);
        splitData(rules, upTempView, tempView);
    }

    private void splitData(List<DataSplitDTO> rules, String upTempView, String tempView) throws Exception {
        Dataset<Row> dataset = session.table(upTempView);
        if (dataset == null || dataset.count() <= 0) {
            logger.warn("数据拆分节点的源数据记录为空：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            return;
        }
        if (rules == null || rules.isEmpty()) {
            logger.warn("数据拆分节点配置的拆分保留字段规则为空：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        List<String> columns = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<Column> cols = rules.stream().filter(it -> StringUtils.isNotBlank(it.getCode()) &&
                it.getReserved() != null && it.getReserved() && columns.contains(it.getCode()))
                .map(r -> col(r.getCode())).collect(Collectors.toList());
        if (cols == null || cols.isEmpty()) {
            logger.warn("数据拆分节点配置的拆分保留字段为空：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        dataset = dataset.select((Seq) wrapRefArray(cols.toArray()));
        dataset.createOrReplaceTempView(tempView);
        session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
    }


  /*  public static void main(String[] args) {
        List<String> list = new ArrayList<>();
        list.add("{\"F_Name\":\"朱波\",\"EmployCertificatelist\":\"{\\\"F_CertificateTypeName\\\":\\\"危险货物运输押运人员\\\",\\\"F_ReputationGradeName\\\":\\\"AAA级\\\",\\\"F_ReputationGrade\\\":\\\"1\\\"}\",\"F_UserPositionName\":\"押运员,驾驶员\",\"F_SexName\":\"男\",\"F_Phone\":\"13408818441\"}");
        list.add("{\"F_Name\":\"朱兵\",EmployCertificatelist\":\"{\\\"F_CertificateTypeName\\\":\\\"危险货物运输押运人员\\\",\\\"F_ReputationGradeName\\\":\\\"AAA级\\\",\\\"F_ReputationGrade\\\":\\\"1\\\"}\",\"F_UserPositionName\":\"押运员,驾驶员\",\"F_SexName\":\"男\",\"F_Phone\":\"15126265988\"}");
        list.add("{\"F_Name\":\"朱斌\",\"EmployCertificatelist\":\"{\\\"F_CertificateTypeName\\\":\\\"危险货物运输押运人员\\\",\\\"F_ReputationGradeName\\\":\\\"AAA级\\\",\\\"F_ReputationGrade\\\":\\\"1\\\"}\",\"F_UserPositionName\":\"押运员\",\"F_SexName\":\"男\",\"F_Phone\":\"15125025625\"}");
        SparkSession sparkSession = SparkSession.builder()
                .config("date", LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE))
                .config("spark.sql.debug.maxToStringFields", "100")
                .master("local[*]")
                .getOrCreate();
        JavaRDD<String> javaRDD = new JavaSparkContext(sparkSession.sparkContext()).parallelize(list);
        // 注册成表
        Dataset<Row> dataset = sparkSession.read().json(javaRDD);
        dataset.printSchema();
        dataset.show();
        dataset.select(functions.explode(split(col("a"), ",")));
        dataset.schema();
        dataset = dataset.select(col("F_Name"), col("F_Phone"), col("F_SexName"), col("F_UserPositionName"),
                col("EmployCertificatelist"), col("a").getItem("F_CertificateTypeName").as("F_CertificateTypeName"),
                col("a").getField("aa").as("F_ReputationGrade"),
                col("a").getItem("F_ReputationGradeName").as("F_ReputationGradeName"));

        dataset.show();
        System.err.println(Integer.toBinaryString(Integer.parseInt("111", 2) + Integer.parseInt("111", 2)));
        sparkSession.close();
       *//* List<String> list = new ArrayList<>();
        list.add("{\"F_Name\":\"朱波\",\"EmployCertificatelist\":[{\"F_CertificateTypeName\":\"危险货物运输押运人员\",\"F_ReputationGradeName\":\"AAA级\",\"F_ReputationGrade\":\"1\"},{\"F_CertificateTypeName\":\"道危险货物运输驾驶员\",\"F_ReputationGradeName\":\"AAA级\",\"F_ReputationGrade\":\"1\"}],\"F_UserPositionName\":\"押运员,驾驶员\",\"F_SexName\":\"男\",\"F_Phone\":\"13408818441\"}");
        list.add("{\"F_Name\":\"朱兵\",\"EmployCertificatelist\":[{\"F_CertificateTypeName\":\"危险货物运输押运人员\",\"F_ReputationGradeName\":\"AA级\",\"F_ReputationGrade\":\"2\"},{\"F_CertificateTypeName\":\"路危险货物运输驾驶员\",\"F_ReputationGradeName\":\"AA级\",\"F_ReputationGrade\":\"2\"}],\"F_UserPositionName\":\"押运员,驾驶员\",\"F_SexName\":\"男\",\"F_Phone\":\"15126265988\"}");
        list.add("{\"F_Name\":\"朱斌\",\"EmployCertificatelist\":[{\"F_CertificateTypeName\":\"危险货物运输押运人员\",\"F_ReputationGradeName\":\"AA级\",\"F_ReputationGrade\":\"2\"},{\"F_CertificateTypeName\":\"爆炸品道路运输押运员\",\"F_ReputationGradeName\":\"AA级\",\"F_ReputationGrade\":\"2\"}],\"F_UserPositionName\":\"押运员\",\"F_SexName\":\"男\",\"F_Phone\":\"15125025625\"}");
        SparkSession sparkSession = SparkSession.builder()
                .config("date", LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE))
                .config("spark.sql.debug.maxToStringFields", "100")
                .master("local[*]")
                .getOrCreate();
        JavaRDD<String> javaRDD = new JavaSparkContext(sparkSession.sparkContext()).parallelize(list);
        // 注册成表
        Dataset<Row> dataset = sparkSession.read().json(javaRDD);
        dataset.printSchema();
        dataset.show();
        dataset.select(functions.explode(split(col("a"), ",")));
        dataset.schema();
        dataset = dataset.select(col("F_Name"), col("F_Phone"), col("F_SexName"), col("F_UserPositionName"),
                col("EmployCertificatelist"), col("a").getItem("F_CertificateTypeName").as("F_CertificateTypeName"),
                col("a").getField("aa").as("F_ReputationGrade"),
                col("a").getItem("F_ReputationGradeName").as("F_ReputationGradeName"));

        dataset.show();
        System.err.println(Integer.toBinaryString(Integer.parseInt("111", 2) + Integer.parseInt("111", 2)));
        sparkSession.close();*//*

    }*/
}
