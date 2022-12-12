package com.wk.data.spark.service.govern.executor;

import com.alibaba.fastjson.JSON;
import com.wk.data.etl.facade.govern.dto.TaskNodeDTO;
import com.wk.data.etl.facade.govern.dto.analysis.Dislocation;
import com.wk.data.etl.facade.govern.dto.analysis.StorageField;
import com.wk.data.etl.facade.govern.enums.OperationRule;
import com.wk.data.etl.facade.govern.enums.SortDirection;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.wk.data.etl.facade.govern.enums.OperationRule.BACK;
import static org.apache.spark.sql.functions.*;
import static scala.Predef.wrapRefArray;

/**
 * @program: data-spark-job
 * @description: 窗口函数，错位运算
 * @author: gwl
 * @create: 2022-07-26 14:59
 **/
public class DataLagService extends Transform implements Serializable, scala.Serializable {

    private static Logger logger = LoggerFactory.getLogger(DataLagService.class);

    private TaskNodeDTO node;

    private SparkSession session;

    public DataLagService(TaskNodeDTO node, SparkSession sparkSession) {
        this.node = node;
        this.session = sparkSession;
    }

    @Override
    public void execute() throws Exception {
        List<String> dns = node.getDependNumber();
        String upTempView = dns.stream().filter(StringUtils::isNotBlank).findFirst().get();
        String tempView = node.getNumber();
        Dislocation dislocation = JSON.parseObject(JSON.toJSONString(node.getData()), Dislocation.class);
        datalag(dislocation, upTempView, tempView);
    }

    private void datalag(Dislocation dis, String upTempView, String tempView) {
        Dataset<Row> dataset = session.table(upTempView);
        if (dataset == null || dataset.count() <= 0) {
            logger.warn("错误运算节点的源数据记录为空：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            return;
        }
        if (dis == null || StringUtils.isBlank(dis.getOrderBy()) || dis.getDirection() == null ||
                dis.getOperationRule() == null || dis.getStorageFields() == null) {
            logger.warn("错误运算节点配置的规则为空：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        List<StorageField> fields = dis.getStorageFields().stream().filter(it ->
                StringUtils.isNotBlank(it.getSourceCode()) && StringUtils.isNotBlank(it.getNewCode())
                ).collect(Collectors.toList());
        if (fields.isEmpty()) {
            logger.warn("错误运算节点配置的存储字段规则不完整：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        Column order = dis.getDirection()== SortDirection.ASC ? asc(dis.getOrderBy()) : desc(dis.getOrderBy());
        WindowSpec window = Window.orderBy(order);
        List<String> pbs = null;
        if (dis.getPartitions() != null) {
            pbs = dis.getPartitions().stream().filter(StringUtils::isNotBlank).distinct().collect(Collectors.toList());
        }
        if (pbs != null && !pbs.isEmpty()) {
            window.partitionBy((Seq) wrapRefArray(dis.getPartitions().toArray()));
        }
        OperationRule ore = dis.getOperationRule();
        List<String> colNames = new ArrayList<>();
        List<Column> cols = new ArrayList<>();
        for (StorageField field : fields) {
            Column sc = field.getScale() == null || field.getScale() < 0 ? lit(0) : lit(field.getScale());
            String fn = field.getSourceCode();
            Column nc = lag(fn, 1).over(window);
            colNames.add(fn);
            if (ore == BACK) {
                cols.add(callUDF("subtract", col(fn), nc, sc).as(field.getNewCode()));
            } else {
                cols.add(callUDF("subtract", nc, col(fn), sc).as(field.getNewCode()));
            }
        }
        dataset = dataset.withColumns((Seq) wrapRefArray(colNames.toArray()), (Seq) wrapRefArray(cols.toArray()));
        dataset.createOrReplaceTempView(tempView);
        session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
    }

   /* public static void main(String[] args) {
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

        spark.sqlContext().udf().register("subtract", MathUDF.subtract(), DataTypes.StringType);
        StructType schema = new StructType(new StructField[]{
                createStructField("id", IntegerType, false),
                createStructField("code", DoubleType, false),
                createStructField("tt", DoubleType, false),
                createStructField("nn", IntegerType, false)
        });

        Dataset<Row> dataset = spark.createDataFrame(data, schema);
        WindowSpec window = Window.*//*(col("id")).*//*orderBy(col("code"));
        functions.callUDF("subtract", col("a"), col("b"), lit(2));
        dataset = dataset.withColumn("covar", callUDF("subtract", lag("code", 1).over(window), col("code"), lit(2)).cast(DoubleType));
        dataset.show();
    }*/

}
