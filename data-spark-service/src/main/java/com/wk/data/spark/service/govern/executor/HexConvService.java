package com.wk.data.spark.service.govern.executor;

import com.alibaba.fastjson.JSON;
import com.wk.data.etl.facade.govern.dto.TaskNodeDTO;
import com.wk.data.etl.facade.govern.dto.analysis.HexConv;
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
import static org.apache.spark.sql.functions.conv;
import static scala.Predef.wrapRefArray;

/**
 * @program: data-spark-job
 * @description: 进制转换
 * @author: gwl
 * @create: 2022-07-26 14:58
 **/
public class HexConvService extends Transform implements Serializable, scala.Serializable {

    private static Logger logger = LoggerFactory.getLogger(HexConvService.class);

    private TaskNodeDTO node;

    private SparkSession session;

    public HexConvService(TaskNodeDTO node, SparkSession sparkSession) {
        this.node = node;
        this.session = sparkSession;
    }

    @Override
    public void execute() throws Exception {
        List<String> dns = node.getDependNumber();
        String upTempView = dns.stream().filter(StringUtils::isNotBlank).findFirst().get();
        String tempView = node.getNumber();
        List<HexConv> rules = JSON.parseArray(JSON.toJSONString(node.getData()), HexConv.class);
        hexconv(rules, upTempView, tempView);
    }

    private void hexconv(List<HexConv> rules, String upTempView, String tempView) {
        Dataset<Row> dataset = session.table(upTempView);
        if (dataset == null || dataset.count() <= 0) {
            logger.warn("进制转换节点的源数据记录为空：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            return;
        }
        if (rules == null || rules.isEmpty()) {
            logger.warn("进制转换节点配置的规则为空：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        List<String> columns = new ArrayList<>(Arrays.asList(dataset.columns()));
        rules = rules.stream().filter(it -> it != null && StringUtils.isNotBlank(it.getScode())
                && columns.contains(it.getScode()) && it.getBeforBase() != null && it.getAfterBase() != null
                ).collect(Collectors.toList());
        if ((rules.isEmpty())) {
            logger.warn("进制转换节点配置的规则不完整：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        List<String> colNames = new ArrayList<>();
        List<Column> cols = new ArrayList<>();
        for (HexConv hc : rules) {
            String alias = StringUtils.isBlank(hc.getTcode()) ? hc.getScode() : hc.getTcode().trim();
            Column column = conv(col(hc.getScode()), hc.getBeforBase().getCode(), hc.getAfterBase().getCode()).as(alias);
            cols.add(column);
            colNames.add(hc.getScode());
        }
        columns.stream().filter(it -> !colNames.contains(it)).forEach(c -> cols.add(col(c)));
        dataset = dataset.select((Seq) wrapRefArray(cols.toArray()));
        dataset.createOrReplaceTempView(tempView);
        session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
    }

    /*public static void main(String[] args) {
        //1、创建一个SparkSession
        SparkSession spark = SparkSession.builder().appName("conv").master("local")
                .getOrCreate();

        //2、加载训练数据样本
        List<Row> data = Arrays.asList(
                RowFactory.create("0", 0.22221, 0.5, 0),
                RowFactory.create("0", 0.1122122, 0.5, 0),
                RowFactory.create("0", 0.1112222, 0.5, 0),
                RowFactory.create("0", 0.112222, 0.5, 0),
                RowFactory.create("0", 0.123333, 0.5, 0),
                RowFactory.create("0", 0.1232323, 0.5, 0),
                RowFactory.create("1", 100.09988, 1.5, 0),
                RowFactory.create("1", 100.33777, 1.5, 0),
                RowFactory.create("1", 100.334d, 1.0, 0),
                RowFactory.create("1", 100d, 1.0, 0),
                RowFactory.create("1", 100d, 1.0, 0),
                RowFactory.create("1", 100d, 1.0, 0),
                RowFactory.create("10", 25.33336, 4.0, 2),
                RowFactory.create("10", 26.223323, 4.5, 3),
                RowFactory.create("10", 27.42112, 4.5, 3),
                RowFactory.create("10", 28.2122, 4.5, 3),
                RowFactory.create("10", 22.2, 4.5, 3),
                RowFactory.create("10", 24.2, 4.5, 3),
                RowFactory.create("10", 22.1, 5.5, 1),
                RowFactory.create("11", 320.0, 9d, 2),
                RowFactory.create("11", 320.0, 9d, 2),
                RowFactory.create("11", 311.0, 9d, 2),
                RowFactory.create("11", 309.0, 9.6, 0),
                RowFactory.create("11", 307.0, 9.6, 0),
                RowFactory.create("11", 300.1, 9.9, 1)

        );
        StructType schema = new StructType(new StructField[]{
                createStructField("id", StringType, false),
                createStructField("code", DoubleType, false),
                createStructField("tt", DoubleType, false),
                createStructField("nn", IntegerType, false)
        });
        Dataset<Row> dataset = spark.createDataFrame(data, schema);
        dataset = dataset.select(
                functions.conv(col("code"), 10, 2).as("c"),
                functions.conv(col("id"), 2, 10).as("d")
        );
        dataset.printSchema();
        dataset.show();
        spark.close();
    }*/

}
