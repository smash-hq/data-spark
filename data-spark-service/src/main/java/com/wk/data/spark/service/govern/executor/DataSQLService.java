package com.wk.data.spark.service.govern.executor;

import com.alibaba.fastjson.JSON;
import com.wk.data.etl.facade.govern.dto.TaskNodeDTO;
import com.wk.data.etl.facade.govern.dto.analysis.SQLEditor;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * @program: data-spark-job
 * @description: SQL处理
 * @author: gwl
 * @create: 2022-07-26 14:59
 **/
public class DataSQLService extends Transform implements Serializable, scala.Serializable {

    private static Logger logger = LoggerFactory.getLogger(DataSQLService.class);

    private TaskNodeDTO node;

    private SparkSession session;

    public DataSQLService(TaskNodeDTO node, SparkSession sparkSession) {
        this.node = node;
        this.session = sparkSession;
    }

    @Override
    public void execute() throws Exception {
        String tempView = node.getNumber();
        SQLEditor sqlEditor = JSON.parseObject(JSON.toJSONString(node.getData()), SQLEditor.class);
        executeSql(sqlEditor, tempView);
    }

    private void executeSql(SQLEditor sqlEditor, String tempView) throws Exception {
        if (sqlEditor == null || StringUtils.isBlank(sqlEditor.getSql())) {
            logger.warn("SQL运算节点配置的规则为空：{}", JSON.toJSONString(node));
            Dataset<Row> dataset = session.createDataset(new ArrayList<>(), Encoders.bean(Row.class));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        Dataset<Row> dataset = session.sql(sqlEditor.getSql().trim());
        dataset.createOrReplaceTempView(tempView);
        session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
    }

/*    public static void main(String[] args) {
        //1、创建一个SparkSession
        SparkSession spark = SparkSession.builder().appName("NaiveBayes").master("local")
                .getOrCreate();

        //2、加载训练数据样本
        List<Row> data = Arrays.asList(
                RowFactory.create(0, 0.22221, 0.5, 0),
                RowFactory.create(0, 0.1122122, 0.5, 0),
                RowFactory.create(0, 0.1112222, 0.5, 0),
                RowFactory.create(0, 0.112222, 0.5, 0),
                RowFactory.create(0, 0.123333, 0.5, 0),
                RowFactory.create(0, 0.1232323, 0.5, 0),
                RowFactory.create(1, 100.09988, 1.5, 0),
                RowFactory.create(1, 100.33777, 1.5, 0),
                RowFactory.create(1, 100.334d, 1.0, 0),
                RowFactory.create(1, 100d, 1.0, 0),
                RowFactory.create(1, 100d, 1.0, 0),
                RowFactory.create(1, 100d, 1.0, 0),
                RowFactory.create(2, 25.33336, 4.0, 2),
                RowFactory.create(2, 26.223323, 4.5, 3),
                RowFactory.create(2, 27.42112, 4.5, 3),
                RowFactory.create(2, 28.2122, 4.5, 3),
                RowFactory.create(2, 22.2, 4.5, 3),
                RowFactory.create(2, 24.2, 4.5, 3),
                RowFactory.create(2, 22.1, 5.5, 1),
                RowFactory.create(3, 320.0, 9d, 2),
                RowFactory.create(3, 320.0, 9d, 2),
                RowFactory.create(3, 311.0, 9d, 2),
                RowFactory.create(3, 309.0, 9.6, 0),
                RowFactory.create(3, 307.0, 9.6, 0),
                RowFactory.create(3, 300.1, 9.9, 1)

        );
        StructType schema = new StructType(new StructField[]{
                createStructField("id", IntegerType, false),
                createStructField("code", DoubleType, false),
                createStructField("tt", DoubleType, false),
                createStructField("nn", IntegerType, false)
        });
        Dataset<Row> train = spark.createDataFrame(data, schema);

        train.createOrReplaceTempView("train");

        spark.sql("SELECT AVG(tt) avgt, MAX(code) maxc, id  FROM train GROUP BY id").show();
        spark.close();
    }*/

}
