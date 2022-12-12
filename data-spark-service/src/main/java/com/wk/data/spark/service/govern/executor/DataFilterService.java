package com.wk.data.spark.service.govern.executor;

import com.alibaba.fastjson.JSON;
import com.wk.data.etl.facade.govern.dto.TaskNodeDTO;
import com.wk.data.etl.facade.govern.dto.filter.DataFilterDTO;
import com.wk.data.etl.facade.govern.dto.filter.FilterCol;
import com.wk.data.etl.facade.govern.enums.FilterModeEnum;
import com.wk.data.etl.facade.govern.enums.FilterTypeEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

/**
 * @program: data-spark-job
 * @description: 数据去重
 * @author: gwl
 * @create: 2021-12-10 15:33
 **/
public class DataFilterService extends Transform implements Serializable, scala.Serializable {

    private static Logger logger = LoggerFactory.getLogger(DataFilterService.class);

    private TaskNodeDTO node;

    private SparkSession session;

    private static String tempSort = "temp_filed_length_sort";

    public DataFilterService(TaskNodeDTO node, SparkSession sparkSession) {
        this.node = node;
        this.session = sparkSession;
    }

    @Override
    public void execute() throws Exception {
        List<String> dns = node.getDependNumber();
        String upTempView = dns.stream().filter(StringUtils::isNotBlank).findFirst().get();
        String tempView = node.getNumber();
        DataFilterDTO filter = JSON.parseObject(JSON.toJSONString(node.getData()), DataFilterDTO.class);
        filterData(filter, upTempView, tempView);
    }

    private void filterData(DataFilterDTO filter, String upTempView, String tempView) throws Exception {
        Dataset<Row> dataset = session.table(upTempView);
        if (dataset == null || dataset.count() <= 0) {
            logger.warn("数据去重节点的源数据记录为空：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            return;
        }
        if (filter == null || filter.getCols() == null) {
            logger.warn("数据去重节点配置的去重规则为空：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<String> fcols = filter.getCols().stream().filter(it -> it != null && it.isSelectd() &&
                StringUtils.isNotBlank(it.getCode())).map(FilterCol::getCode).distinct().collect(Collectors.toList());
        fcols.retainAll(cols);
        if (fcols.isEmpty()) {
            logger.warn("数据去重节点配置的过滤字段跟源数据字段不匹配：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        String sortMode = filter.getMode();
        String sortType = filter.getType();
        String sortCol = filter.getCode();
        if (StringUtils.isBlank(sortType) || StringUtils.isBlank(sortCol) || StringUtils.isBlank(sortMode) ||
                !cols.contains(sortCol)) {
            dataset = dataset.dropDuplicates(fcols.toArray(new String[fcols.size()]));
        } else {
            FilterTypeEnum ftype = FilterTypeEnum.getEnumByEnName(sortType);
            FilterModeEnum fmode = FilterModeEnum.getEnumByEnName(sortMode);
            switch (ftype) {
                case STRING:
                    dataset = dataset.withColumn(tempSort, functions.callUDF("getLength", dataset.col(sortCol)
                            .cast(DataTypes.StringType)))
                            .orderBy(FilterModeEnum.LESS.equals(fmode) ? col(tempSort).asc() : col(tempSort).desc())
                            .dropDuplicates(fcols.toArray(new String[fcols.size()]));
                    break;
                case DATE:
                    Column dateSort = FilterModeEnum.GREATER.equals(fmode) ? col(sortCol).asc() : col(sortCol).desc();
                    dataset = dataset.orderBy(dateSort).dropDuplicates(fcols.toArray(new String[fcols.size()]));
                    break;
                case NUMBER:
                    Column numSort = FilterModeEnum.MIN.equals(fmode) ? col(sortCol).asc() : col(sortCol).desc();
                    dataset = dataset.orderBy(numSort).dropDuplicates(fcols.toArray(new String[fcols.size()]));
                    break;
                default:
                    dataset = dataset.dropDuplicates(fcols.toArray(new String[fcols.size()]));
                    break;
            }
        }
        dataset.createOrReplaceTempView(tempView);
        session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
    }

   /* public static void main(String[] args) {
        Map<String, Object> map = new HashMap<>();
        map.putAll(ImmutableMap.of("A", 1, "B", "b", "C", "c1", "D", "2021-12-23 10:00:01"));
        Map<String, Object> map1 = new HashMap<>();
        map1.putAll(ImmutableMap.of("A", 1, "B", "bb", "C", "c2", "D", ""));
        Map<String, Object> map2 = new HashMap<>();
        map2.putAll(ImmutableMap.of("A", 2, "B", "bbb", "C", "c3", "D", "1622001255022522"));
        Map<String, Object> map3 = new HashMap<>();
        map3.putAll(ImmutableMap.of("A", 2, "B", "bbb", "C", "c44", "D", "2021-12-23 12:12:01"));
        Map<String, Object> map4 = new HashMap<>();
        map4.putAll(ImmutableMap.of("A", 2, "B", "bbb", "C", "c444", "D", "2021-12-25"));
        List<String> list = new ArrayList<>();
        list.add(JSON.toJSONString(map));
        list.add(JSON.toJSONString(map1));
        list.add(JSON.toJSONString(map2));
        list.add(JSON.toJSONString(map3));
        list.add(JSON.toJSONString(map4));
        SparkSession sparkSession = SparkSession.builder()
                .config("date", LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE))
                .config("spark.sql.debug.maxToStringFields", "100")
                .master("local[*]")
                .getOrCreate();
        JavaRDD<String> javaRDD = new JavaSparkContext(sparkSession.sparkContext()).parallelize(list);
        // 注册成表
        Dataset<Row> dataset = sparkSession.read().json(javaRDD);
        dataset.createOrReplaceTempView("ab");
        sparkSession.catalog().cacheTable("ab");
        dataset.show();
        dataset = dataset.select(col("A"), col("B"), col("C"),
                col("D").cast(DataTypes.TimestampType))
                .withColumn("S", functions.callUDF("getLength", dataset.col("C").cast( DataTypes.StringType)));
        sparkSession.sqlContext().udf().register("getLength", getLength(), DataTypes.IntegerType);
        dataset = dataset.orderBy(col("S").asc()).dropDuplicates("A");
        dataset.show();
        dataset.createOrReplaceTempView("cd");
        sparkSession.catalog().cacheTable("cd");
        sparkSession.table("ab").show();
        sparkSession.table("cd").show();
        sparkSession.close();

    }*/

}
