package com.wk.data.spark.service.govern.executor;

import com.alibaba.fastjson.JSON;
import com.wk.data.etl.facade.govern.dto.TaskNodeDTO;
import com.wk.data.etl.facade.govern.dto.integrate.DataIntegrate;
import com.wk.data.etl.facade.govern.dto.integrate.JoinTable;
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

import static com.wk.data.spark.infrastructure.util.General.ALIAS_FORMAT;
import static com.wk.data.spark.infrastructure.util.General.LEFT_OUTER;
import static org.apache.spark.sql.functions.col;
import static scala.Predef.wrapRefArray;

/**
 * @program: data-spark-job
 * @description: 数据集成
 * @author: gwl
 * @create: 2021-12-10 15:33
 **/
public class DataJoinService extends Transform implements Serializable, scala.Serializable {

    private static Logger logger = LoggerFactory.getLogger(DataJoinService.class);

    private TaskNodeDTO node;

    private SparkSession session;


    public DataJoinService(TaskNodeDTO node, SparkSession sparkSession) {
        this.node = node;
        this.session = sparkSession;
    }

    @Override
    public void execute() throws Exception {
        String tempView = node.getNumber();
        DataIntegrate integrate = JSON.parseObject(JSON.toJSONString(node.getData()), DataIntegrate.class);
        joinData(integrate, tempView);
    }

    private void joinData(DataIntegrate integrate, String tempView) throws Exception {
        if (integrate == null || StringUtils.isBlank(integrate.getPreNumber()) || StringUtils.isBlank(integrate.getNickname())) {
            logger.error("数据集成节点配置的主表信息为空：{}", JSON.toJSONString(node));
            Dataset<Row> dataset = session.createDataset(new ArrayList<>(), Encoders.bean(Row.class));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        Dataset<Row> dataset = loadDataset(integrate.getPreNumber(), integrate.getNickname());
        if (dataset == null || dataset.count() <= 0) {
            logger.error("数据集成中的主表【{}：{}】数据记录为空", integrate.getName(), integrate.getCode());
            dataset.createOrReplaceTempView(tempView);
            return;
        }
        List<JoinTable> joins = integrate.getJoins().stream().filter(it ->
                StringUtils.isNotBlank(it.getPreNumber()) && StringUtils.isNotBlank(it.getNickname()) &&
                        StringUtils.isNotBlank(it.getLeftField()) && StringUtils.isNotBlank(it.getRightField()))
                .collect(Collectors.toList());
        if (joins.isEmpty()) {
            logger.error("数据集成中配置的关联表信息为空：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        for (int i = 0; i < joins.size(); i++) {
            JoinTable joinTable = joins.get(i);
            Dataset<Row> joinDataset = loadDataset(joinTable.getPreNumber(), joinTable.getNickname());
            if (dataset == null || dataset.count() <= 0) {
                logger.warn("数据集成中的主表【{}：{}】数据记录为空", integrate.getName(), integrate.getCode());
            }
            List<String> master = Arrays.asList(dataset.columns());
            List<String> slave = Arrays.asList(joinDataset.columns());
            String left = String.format(ALIAS_FORMAT, integrate.getNickname(), joinTable.getLeftField());
            String right = String.format(ALIAS_FORMAT, joinTable.getNickname(), joinTable.getRightField());
            if (!master.contains(left) || !slave.contains(right)) {
                logger.error("数据归并节点配置的字段规则检测为空：{}", JSON.toJSONString(joinTable));
            }
            dataset = dataset.join(joinDataset, dataset.col(left).equalTo(joinDataset.col(right)), LEFT_OUTER);
        }
        dataset.createOrReplaceTempView(tempView);
        session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
    }

    private Dataset<Row> loadDataset(String table, String alias) {
        Dataset<Row> dataset = session.table(table);
        if (dataset == null) {
            return null;
        }
        String[] fields = dataset.columns();
        List<Column> cols = Arrays.stream(fields).map(it -> col(it).as(String.format(ALIAS_FORMAT, alias, it)))
                .collect(Collectors.toList());
        dataset = dataset.select((Seq) wrapRefArray(cols.toArray()));
        return dataset;
    }


  /*  public static void main(String[] args) throws Exception {
        SparkSession sparkSession = SparkSession.builder()
                .config("date", LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE))
                .config("spark.sql.debug.maxToStringFields", "100")
                .master("local[*]")
                .getOrCreate();
        JavaRDD<String> javaRDD1 = new JavaSparkContext(sparkSession.sparkContext()).parallelize(initList(new String[]{"id", "name", "age"}, 3));
        Dataset<Row> dataset1 = sparkSession.read().json(javaRDD1);
        dataset1.show();
        dataset1.createOrReplaceTempView("table1");

        JavaRDD<String> javaRDD2 = new JavaSparkContext(sparkSession.sparkContext()).parallelize(initList(new String[]{"id", "name", "pre", "levll", "pid"}, 5));
        Dataset<Row> dataset2 = sparkSession.read().json(javaRDD2);
        dataset2.show();
        dataset2.createOrReplaceTempView("table2");

        JavaRDD<String> javaRDD3 = new JavaSparkContext(sparkSession.sparkContext()).parallelize(initList(new String[]{"id", "name", "card", "life", "pid", "bir"}, 7));
        Dataset<Row> dataset3 = sparkSession.read().json(javaRDD3);
        dataset3.show();
        dataset3.createOrReplaceTempView("table3");

        List<JoinTable> joins = new ArrayList<>();
        joins.add(JoinTable.builder().preNumber("table2").nickname("t2").leftField("id").rightField("pid").build());
        joins.add(JoinTable.builder().preNumber("table3").nickname("t3").leftField("id").rightField("pid").build());
        DataIntegrate integrate = DataIntegrate.builder().preNumber("table1").nickname("t1").joins(joins).build();
        TaskNodeDTO node = TaskNodeDTO.builder().number("union").data(integrate).build();
        DataJoinService join = new DataJoinService(node, sparkSession);
        join.executeDataJoin();
        Dataset<Row> dataset = sparkSession.table("union");
        String[] fields = dataset.columns();
        List<Column> cols = Arrays.stream(fields).map(it -> col(it))
                .collect(Collectors.toList());
        dataset = dataset.select((Seq) wrapRefArray(cols.toArray()));
        System.err.println(dataset.count());
        dataset.show();
        sparkSession.close();

    }

    private static List<String> initList(String[] keys, int size) {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Map<String, Object> map = new HashMap<>();
            for (int j = 0; j < keys.length; j++) {
                map.put(keys[j], randomObjectValue(j));
            }
            list.add(JSON.toJSONString(map));
        }
        return list;
    }

    private static Object randomObjectValue(int j) {
        switch (j) {
            case 0:
            case 4:
                return Math.random();
            case 5:
                return LocalTime.now();
            default:
                return UUID.randomUUID().toString();
        }
    }*/
}
