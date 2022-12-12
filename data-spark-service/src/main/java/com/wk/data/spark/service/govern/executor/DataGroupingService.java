package com.wk.data.spark.service.govern.executor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wk.data.etl.facade.govern.dto.TaskNodeDTO;
import com.wk.data.etl.facade.govern.dto.grouping.GroupingStatisticsDTO;
import com.wk.data.etl.facade.govern.dto.grouping.StatisticDTO;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static scala.Predef.wrapRefArray;

/**
 * @Created: smash_hq at 14:08 2022/7/26
 * @Description: analysis model
 */
public class DataGroupingService extends Transform implements Serializable, scala.Serializable {
    private static Logger logger = LoggerFactory.getLogger(DataGroupingService.class);

    private TaskNodeDTO node;

    private SparkSession session;

    public DataGroupingService(TaskNodeDTO node, SparkSession sparkSession) {
        this.node = node;
        this.session = sparkSession;
    }

    @Override
    public void execute() throws Exception {
        List<String> dns = node.getDependNumber();
        String upTempView = dns.stream().filter(StringUtils::isNotBlank).findFirst().get();
        String tempView = node.getNumber();
        GroupingStatisticsDTO groupingStatisticsDTO = JSONObject.parseObject(JSON.toJSONString(node.getData()), GroupingStatisticsDTO.class);
        grouping(groupingStatisticsDTO, upTempView, tempView);
    }

    private void grouping(GroupingStatisticsDTO groupingStatisticsDTO, String upTempView, String tempView) {
        Dataset<Row> dataset = session.table(upTempView);
        List<String> groupBy = groupingStatisticsDTO.getGroupBy();
        List<StatisticDTO> statistics = groupingStatisticsDTO.getStatistics();

        RelationalGroupedDataset relationalGroupedDataset = relationGroupedDataset(groupBy, dataset);
        List<String> countList = new ArrayList<>();
        List<String> sumList = new ArrayList<>();
        List<String> avgList = new ArrayList<>();
        List<String> maxList = new ArrayList<>();
        List<String> minList = new ArrayList<>();
        getStatistic(statistics, countList, sumList, avgList, maxList, minList);

        Dataset<Row> count = countDataset(countList, relationalGroupedDataset);
        Dataset<Row> min = minDataset(minList, relationalGroupedDataset);
        Dataset<Row> max = maxDataset(maxList, relationalGroupedDataset);
        Dataset<Row> sum = sumDataset(sumList, relationalGroupedDataset);
        Dataset<Row> avg = avgDataset(avgList, relationalGroupedDataset);

        Dataset<Row> rowDataset = count;
        Seq<String> seq = (Seq) wrapRefArray(groupBy.toArray());
        if (min != null) {
            rowDataset = rowDataset.join(min, seq);
        }
        if (max != null) {
            rowDataset = rowDataset.join(max, seq);
        }
        if (sum != null) {
            rowDataset = rowDataset.join(sum, seq);
        }
        if (avg != null) {
            rowDataset = rowDataset.join(avg, seq);
        }
        // 取别名
        rowDataset = aliasColumn(statistics, sumList, avgList, maxList, minList, rowDataset);
        //
        rowDataset = selectAlias(groupBy, statistics, rowDataset);
        // 添加时间戳
        Boolean isAddDate = groupingStatisticsDTO.getIsAddDate();
        rowDataset = addDate(groupingStatisticsDTO, rowDataset, isAddDate);

        rowDataset.createOrReplaceTempView(tempView);
        session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
    }

    private Dataset<Row> selectAlias(List<String> groupBy, List<StatisticDTO> statistics, Dataset<Row> rowDataset) {
        List<String> columStrs = statistics.stream().map(StatisticDTO::getCode).collect(Collectors.toList());
        columStrs.addAll(groupBy);
        int size = columStrs.size();
        Column[] columns = new Column[size];
        for (int i = 0; i < size; i++) {
            columns[i] = new Column(columStrs.get(i));
        }
        rowDataset = rowDataset.select(columns);
        return rowDataset;
    }

    private Dataset<Row> addDate(GroupingStatisticsDTO groupingStatisticsDTO, Dataset<Row> rowDataset, Boolean isAddDate) {
        if (isAddDate != null && isAddDate) {
            LocalDateTime localDateTime = LocalDateTime.now();
            String str = localDateTime.format(DateTimeFormatter.ofPattern(groupingStatisticsDTO.getDateFormat()));
            Metadata metadata = Metadata.fromJson(str);
            String dataCode = groupingStatisticsDTO.getDataCode();
            if (StringUtils.isBlank(dataCode)) {
                logger.error("指标统计组件，缺少字段统计输出字段代码");
            }
            Column col = functions.unix_timestamp();
            rowDataset = rowDataset.withColumn(dataCode, col);
        }
        return rowDataset;
    }

    private Dataset<Row> aliasColumn(List<StatisticDTO> statistics,
                                     List<String> sumList, List<String> avgList, List<String> maxList, List<String> minList,
                                     Dataset<Row> rowDataset) {
        for (StatisticDTO statistic : statistics) {
            String toCode = statistic.getCode();
            String byCode = statistic.getByCode();
            Integer mode = statistic.getMode();
            switch (mode) {
                case 0:
                    rowDataset = rowDataset.withColumn(toCode, rowDataset.col(byCode));
                    break;
                case 1:
                    rowDataset = rowDataset.withColumn(toCode, rowDataset.col("count"));
                    break;
                case 2:
                    rowDataset = rowDataset.withColumn(toCode, rowDataset.col("sum(" + byCode + ")"));
                    sumList.add(byCode);
                    break;
                case 3:
                    rowDataset = rowDataset.withColumn(toCode, rowDataset.col("avg(" + byCode + ")"));
                    avgList.add(byCode);
                    break;
                case 4:
                    rowDataset = rowDataset.withColumn(toCode, rowDataset.col("max(" + byCode + ")"));
                    maxList.add(byCode);
                    break;
                case 5:
                    rowDataset = rowDataset.withColumn(toCode, rowDataset.col("min(" + byCode + ")"));
                    minList.add(byCode);
                    break;
                default:
                    break;
            }
        }
        return rowDataset;
    }

//    public static void main(String[] args) {
//        String str = "{\n" +
//                "    \"groupBy\":\n" +
//                "    [\n" +
//                "        \"project_id\",\n" +
//                "        \"flow_id\"\n" +
//                "    ],\n" +
//                "    \"statistics\":\n" +
//                "    [\n" +
//                "        {\n" +
//                "            \"code\": \"sum_project_id\",\n" +
//                "            \"byCode\": \"project_id\",\n" +
//                "            \"mode\": 2\n" +
//                "        },\n" +
//                "        {\n" +
//                "            \"code\": \"sum_exec_id\",\n" +
//                "            \"byCode\": \"exec_id\",\n" +
//                "            \"mode\": 2\n" +
//                "        },\n" +
//                "        {\n" +
//                "            \"code\": \"avg_status\",\n" +
//                "            \"byCode\": \"status\",\n" +
//                "            \"mode\": 3\n" +
//                "        }\n" +
//                "    ]\n" +
//                "}";
//
//        GroupingStatisticsDTO groupingStatisticsDTO = JSONObject.parseObject(str, GroupingStatisticsDTO.class);
//        dataset();
//        grouping(groupingStatisticsDTO, "upTempView", "downTempView");
//
//
//    }

//    public static SparkSession sparkSession() {
//        String date = LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE);
//        return SparkSession.builder()
//                .appName("spark")
//                .config("date", date)
//                .config("spark.sql.debug.maxToStringFields", "100")
//                .master("local[*]")
//                .getOrCreate();
//    }
//
//    private void dataset() {
//        Dataset<Row> dataset = sparkSession().read().format("jdbc")
//                .option("url", "jdbc:mysql://10.50.125.141:3306/test_rule   ?useSSL=false&&characterEncoding=utf8")
//                .option("driver", "com.mysql.cj.jdbc.Driver")
//                .option("user", "root")
//                .option("password", "hxc@069.root_mysql")
//                .option("dbtable", "execution_flows")
//                .load();
//        dataset.createOrReplaceTempView("upTempView");
//    }

    private void getStatistic(List<StatisticDTO> statistics, List<String> countList, List<String> sumList, List<String> avgList, List<String> maxList, List<String> minList) {
        for (StatisticDTO statistic : statistics) {
            String byCode = statistic.getByCode();
            String code = statistic.getCode();
            Integer mode = statistic.getMode();
            switch (mode) {
                case 1:
                    countList.add(byCode);
                    break;
                case 2:
                    sumList.add(byCode);
                    break;
                case 3:
                    avgList.add(byCode);
                    break;
                case 4:
                    maxList.add(byCode);
                    break;
                case 5:
                    minList.add(byCode);
                    break;
                default:
                    break;
            }
        }
    }

    private RelationalGroupedDataset relationGroupedDataset(List<String> groupBy, Dataset<Row> dataset) {
        int size = groupBy.size();
        Column[] columns = new Column[size];
        for (int i = 0; i < size; i++) {
            columns[i] = new Column(groupBy.get(i));
        }
        return dataset.groupBy(columns);
    }

    private Dataset<Row> countDataset(List<String> countList, RelationalGroupedDataset relationalGroupedDataset) {
        return relationalGroupedDataset.count();
    }

    private Dataset<Row> sumDataset(List<String> sumList, RelationalGroupedDataset relationalGroupedDataset) {
        if (sumList.isEmpty()) {
            return null;
        }
        int size = sumList.size();
        String[] strings = new String[size];
        for (int i = 0; i < size; i++) {
            strings[i] = sumList.get(i);
        }
        return relationalGroupedDataset.sum(strings);
    }

    private Dataset<Row> maxDataset(List<String> maxList, RelationalGroupedDataset relationalGroupedDataset) {
        if (maxList.isEmpty()) {
            return null;
        }
        int size = maxList.size();
        String[] strings = new String[size];
        for (int i = 0; i < size; i++) {
            strings[i] = maxList.get(i);
        }
        return relationalGroupedDataset.max(strings);
    }

    private Dataset<Row> minDataset(List<String> minList, RelationalGroupedDataset relationalGroupedDataset) {
        if (minList.isEmpty()) {
            return null;
        }
        int size = minList.size();
        String[] strings = new String[size];
        for (int i = 0; i < size; i++) {
            strings[i] = minList.get(i);
        }
        return relationalGroupedDataset.min(strings);
    }

    private Dataset<Row> avgDataset(List<String> avgList, RelationalGroupedDataset relationalGroupedDataset) {
        if (avgList.isEmpty()) {
            return null;
        }
        int size = avgList.size();
        String[] strings = new String[size];
        for (int i = 0; i < size; i++) {
            strings[i] = avgList.get(i);
        }
        return relationalGroupedDataset.avg(strings);
    }

}
