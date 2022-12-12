package com.wk.data.spark.service.govern.executor;

import com.alibaba.fastjson.JSON;
import com.wk.data.etl.facade.govern.dto.TaskNodeDTO;
import com.wk.data.etl.facade.govern.dto.split.DataExplodeDTO;
import com.wk.data.etl.facade.govern.enums.DFormatEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.*;
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
 * @description: 数据行拆分，数据多维分解
 * @author: gwl
 * @create: 2021-12-10 15:33
 **/
public class DataExplodeService extends Transform implements Serializable, scala.Serializable {

    private static Logger logger = LoggerFactory.getLogger(DataExplodeService.class);

    private TaskNodeDTO node;

    private SparkSession session;


    public DataExplodeService(TaskNodeDTO node, SparkSession sparkSession) {
        this.node = node;
        this.session = sparkSession;
    }

    @Override
    public void execute() throws Exception {
        List<String> dns = node.getDependNumber();
        String upTempView = dns.stream().filter(StringUtils::isNotBlank).findFirst().get();
        String tempView = node.getNumber();
        List<DataExplodeDTO> rules = JSON.parseArray(JSON.toJSONString(node.getData()), DataExplodeDTO.class);
        explodeData(rules, upTempView, tempView);
    }

    private void explodeData(List<DataExplodeDTO> rules, String upTempView, String tempView) throws Exception {
        Dataset<Row> dataset = session.table(upTempView);
        if (dataset == null || dataset.count() <= 0) {
            logger.warn("数据行拆分节点的源数据记录为空：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            return;
        }
        if (rules == null || rules.isEmpty()) {
            logger.warn("数据行拆分节点配置的行拆分规则为空：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        List<String> columns = new ArrayList<>(Arrays.asList(dataset.columns()));
        rules = rules.stream().filter(it -> it != null && StringUtils.isNotBlank(it.getCode())
                && columns.contains(it.getCode()) && it.getFormat() != null && it.getColnums() != null
                && !it.getColnums().isEmpty()).collect(Collectors.toList());
        if ((rules.isEmpty())) {
            logger.warn("数据行拆分节点配置的行拆分规则不完整：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        List<String> explodeCols = new ArrayList<>();
       for (DataExplodeDTO explode : rules) {
            List<String> newcodes = explode.getColnums().stream().filter(it -> it != null &&
                    StringUtils.isNotBlank(it.getCode())).map(com.wk.data.etl.facade.govern.dto.clean.Column::getCode)
                    .distinct().collect(Collectors.toList());
            if (newcodes.isEmpty()) {
                continue;
            }
            explodeCols.addAll(newcodes);
            if (DFormatEnum.JSONARRAY.equals(explode.getFormat())) {
                dataset = dataset.withColumn(explode.getCode(), functions.explode(col(explode.getCode())));
            }
       }
       if (!explodeCols.isEmpty()) {
           List<Column> cols = new ArrayList<>();
           columns.forEach(c -> cols.add(col(c).as(c)));
           explodeCols.forEach(ec -> cols.add(col(ec).as(ec)));
           dataset = dataset.select((Seq) wrapRefArray(cols.toArray()));
       }
        dataset.createOrReplaceTempView(tempView);
        session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
    }


    public static void main(String[] args) {
      /*  List<String> list = new ArrayList<>();
        list.add("{\"name\":\"张三\", \"age\":12, \"gender\": \"男\", \"health\": {\"height\" : 180, \"weight\": 68, \"mode\": \"AA\", \"type\": \"强壮\"}, \"hobby\":[{\"type\":\"读书\", \"time\": 4, \"du\": \"重度\"}, {\"type\":\"爬山\", \"time\": 3, \"du\": \"中度\"}], \"ability\":[{\"skill\":\"独孤九剑\", \"master\": \"令狐冲\", \"founder\": \"独孤求败\"}, {\"skill\":\"吸星大法\", \"master\": \"任我行\", \"founder\": \"未知\"}]}");
        list.add("{\"name\":\"李四\", \"age\":20, \"gender\": \"女\", \"health\": {\"height\" : 160, \"weight\": 68, \"mode\": \"AA\", \"type\": \"强壮\"}, \"hobby\":[{\"type\":\"读书\", \"time\": 4, \"du\": \"重度\"}, {\"type\":\"爬山\", \"time\": 3, \"du\": \"中度\"}], \"ability\":[{\"skill\":\"辟邪剑谱\", \"master\": \"自学\", \"founder\": \"和尚\"}, {\"skill\":\"华山剑法\", \"master\": \"岳不群\", \"founder\": \"未知\"}]}");
        list.add("{\"name\":\"王五\", \"age\":30, \"gender\": \"男\", \"health\": {\"height\" : 170, \"weight\": 68, \"mode\": \"AA\", \"type\": \"强壮\"}, \"hobby\":[{\"type\":\"读书\", \"time\": 4, \"du\": \"重度\"}, {\"type\":\"爬山\", \"time\": 3, \"du\": \"中度\"}], \"ability\":[{\"skill\":\"葵花宝典\", \"master\": \"自学\", \"founder\": \"东方不败\"}]}");
        SparkSession sparkSession = SparkSession.builder()
                .config("date", LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE))
                .config("spark.sql.debug.maxToStringFields", "100")
                .master("local[*]")
                .getOrCreate();
        JavaRDD<String> javaRDD = new JavaSparkContext(sparkSession.sparkContext()).parallelize(list);
        // 注册成表
        Dataset<Row> dataset = sparkSession.read().json(javaRDD);
        dataset.printSchema();
        dataset.schema();
        dataset = dataset.withColumn("hobby", functions.explode(col("hobby")));
        dataset.show();
        dataset = dataset.withColumn("ability", functions.explode(col("ability")));
      *//*  dataset = dataset.select(col("F_Name"), col("F_Phone"), col("F_SexName"), col("F_UserPositionName"),
                col("EmployCertificatelist"), col("a").getItem("F_CertificateTypeName").as("F_CertificateTypeName"),
                col("a").getField("aa").as("F_ReputationGrade"),
                col("a").getItem("F_ReputationGradeName").as("F_ReputationGradeName"));*//*
        String[] cs = dataset.columns();
        List<Column> cols = new ArrayList<>();
        for (String c : cs) {
            cols.add(col(c).as(c));
        }
        String[] incs = new String[]{"health.height", "health.weight", "health.mode", "health.type", "hobby.type", "hobby.time", "hobby.du", "ability.skill", "ability.master", "ability.founder"};
        for (String c : incs) {
            cols.add(col(c).as(c));
        }
        dataset = dataset.select((Seq) wrapRefArray(cols.toArray()));
        dataset.show();
        sparkSession.close();*/

    }
}
