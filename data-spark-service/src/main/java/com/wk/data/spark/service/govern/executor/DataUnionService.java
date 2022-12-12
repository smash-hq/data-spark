package com.wk.data.spark.service.govern.executor;

import com.alibaba.fastjson.JSON;
import com.vcolco.components.basis.exception.BizException;
import com.wk.data.etl.facade.govern.dto.TaskNodeDTO;
import com.wk.data.etl.facade.govern.dto.union.DataUnionDTO;
import com.wk.data.etl.facade.govern.dto.union.FieldMapping;
import com.wk.data.etl.facade.govern.dto.union.UnionMapping;
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
import static org.apache.spark.sql.functions.lit;
import static scala.Predef.wrapRefArray;

/**
 * @program: data-spark-job
 * @description: 数据归并
 * @author: gwl
 * @create: 2021-12-10 15:33
 **/
public class DataUnionService extends Transform implements Serializable, scala.Serializable {

    private static Logger logger = LoggerFactory.getLogger(DataUnionService.class);

    private TaskNodeDTO node;

    private SparkSession session;


    public DataUnionService(TaskNodeDTO node, SparkSession sparkSession) {
        this.node = node;
        this.session = sparkSession;
    }

    @Override
    public void execute() throws Exception {
        String tempView = node.getNumber();
        DataUnionDTO unionRule = JSON.parseObject(JSON.toJSONString(node.getData()), DataUnionDTO.class);
        unionData(unionRule, tempView);
    }

    private void unionData(DataUnionDTO unionRule, String tempView) throws Exception {
        if (unionRule == null || unionRule.getMappings() == null || unionRule.getMappings().isEmpty()) {
            logger.warn("数据归并节点配置的规则为空：{}", JSON.toJSONString(node));
            Dataset<Row> dataset = session.createDataset(new ArrayList<>(), Encoders.bean(Row.class));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        List<UnionMapping> mappings = unionRule.getMappings().stream().filter(it ->
                StringUtils.isNotBlank(it.getInputNumber()) && it.getFields() != null && !it.getFields().isEmpty())
                .collect(Collectors.toList());
        if (mappings.isEmpty()) {
            logger.warn("数据归并节点配置的字段规则检测为空：{}", JSON.toJSONString(node));
            Dataset<Row> dataset = session.createDataset(new ArrayList<>(), Encoders.bean(Row.class));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        if (!isSameOfFields(mappings)) {
            logger.error("数据归并节点配置的字段规则检测为空：{}", JSON.toJSONString(node));
            throw new BizException("数据归并处理配置信息中字段不统一导致无法正确合并数据");
        }
        Dataset<Row> dataset = loadConvert(mappings.get(0));
        for (int i = 1; i < mappings.size(); i++) {
            Dataset<Row> nextDs = loadConvert(mappings.get(i));
            dataset = dataset.union(nextDs);
        }
        dataset.createOrReplaceTempView(tempView);
        session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
    }

    private Dataset<Row> loadConvert(UnionMapping mapping) {
        Dataset<Row> dataset = session.table(mapping.getInputNumber());
        List<FieldMapping> fields = mapping.getFields();
        List<String> columns = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<Column> cols = new ArrayList<>();
        for (FieldMapping f : fields) {
            String out = f.getOutCode();
            if (StringUtils.isBlank(f.getOutCode())) {
                continue;
            }
            String in = f.getInCode();
            if (StringUtils.isBlank(in) || !columns.contains(in)) {
                cols.add(lit(null).as(out));
            } else {
                cols.add(col(in).as(out));
            }
        }
        return dataset.select((Seq) wrapRefArray(cols.toArray()));
    }

    private boolean isSameOfFields(List<UnionMapping> mappings) {
        for (int i = 0; i < mappings.size() - 1; i++) {
            List<String> cur =mappings.get(i).getFields().stream().filter(it -> StringUtils.isNotBlank(it.getOutCode()))
                    .map(FieldMapping::getOutCode).distinct().collect(Collectors.toList());
            List<String> next = mappings.get(i+1).getFields().stream().filter(it -> StringUtils.isNotBlank(it.getOutCode()))
                    .map(FieldMapping::getOutCode).distinct().collect(Collectors.toList());
            if (cur.size() != next.size() || !cur.containsAll(next)) {
                return false;
            }
        }
        return true;
    }

    /*public static void main(String[] args) throws Exception {
        SparkSession sparkSession = SparkSession.builder()
                .config("date", LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE))
                .config("spark.sql.debug.maxToStringFields", "100")
                .master("local[*]")
                .getOrCreate();
        JavaRDD<String> javaRDD1 = new JavaSparkContext(sparkSession.sparkContext()).parallelize(initList(new String[]{"T1A", "T1B", "T1C"}, 3));
        Dataset<Row> dataset1 = sparkSession.read().json(javaRDD1);
        dataset1.show();
        dataset1.createOrReplaceTempView("table1");

        JavaRDD<String> javaRDD2 = new JavaSparkContext(sparkSession.sparkContext()).parallelize(initList(new String[]{"T2A", "T2C", "T2B", "T2D"}, 5));
        Dataset<Row> dataset2 = sparkSession.read().json(javaRDD2);
        dataset2.show();
        dataset2.createOrReplaceTempView("table2");

        JavaRDD<String> javaRDD3 = new JavaSparkContext(sparkSession.sparkContext()).parallelize(initList(new String[]{"T3A", "T3B", "T3C"}, 2));
        Dataset<Row> dataset3 = sparkSession.read().json(javaRDD3);
        dataset3.show();
        dataset3.createOrReplaceTempView("table3");

        List<UnionMapping> mappings = new ArrayList<>();
        mappings.add(UnionMapping.builder().inputNumber("table1").fields(
                Arrays.asList(FieldMapping.builder().outCode("A").build(),
                        FieldMapping.builder().inCode("T1A").outCode("B").build(),
                        FieldMapping.builder().inCode("T1B").outCode("C").build(),
                        FieldMapping.builder().inCode("T1C").outCode("D").build(),
                        FieldMapping.builder().outCode("E").build())
        ).build());
        mappings.add(UnionMapping.builder().inputNumber("table2").fields(
                Arrays.asList(FieldMapping.builder().inCode("T2A").outCode("A").build(),
                        FieldMapping.builder().inCode("T2C").outCode("B").build(),
                        FieldMapping.builder().inCode("T2B").outCode("C").build(),
                        FieldMapping.builder().inCode("T2D").outCode("D").build(),
                        FieldMapping.builder().outCode("E").build())
        ).build());
        mappings.add(UnionMapping.builder().inputNumber("table3").fields(
                Arrays.asList(FieldMapping.builder().inCode("T3A").outCode("A").build(),
                        FieldMapping.builder().outCode("B").build(),
                        FieldMapping.builder().inCode("T3B").outCode("C").build(),
                        FieldMapping.builder().outCode("D").build(),
                        FieldMapping.builder().inCode("T3C").outCode("E").build())
        ).build());
        DataUnionDTO unionRule = DataUnionDTO.builder().mappings(mappings).build();
        TaskNodeDTO node = TaskNodeDTO.builder().number("union").data(unionRule).build();
        DataUnionService union = new DataUnionService(node, sparkSession);
        union.executeDataUnion();
        Dataset<Row> dataset = sparkSession.table("union");
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
                return UUID.randomUUID().toString();
            case 1:
                return Math.random();
            default:
                return LocalTime.now();
        }
    }*/
}
