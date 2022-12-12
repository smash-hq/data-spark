package com.wk.data.spark.service.govern.executor;

import com.alibaba.fastjson.JSON;
import com.wk.data.etl.facade.govern.dto.TaskNodeDTO;
import com.wk.data.etl.facade.govern.dto.convert.ConvertRule;
import com.wk.data.etl.facade.govern.dto.convert.DataConvertDTO;
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
 * @description: 数据转换
 * @author: gwl
 * @create: 2021-12-10 15:33
 **/
public class DataConvertService extends Transform implements Serializable, scala.Serializable {

    private static Logger logger = LoggerFactory.getLogger(DataConvertService.class);

    private TaskNodeDTO node;

    private SparkSession session;


    public DataConvertService(TaskNodeDTO node, SparkSession sparkSession) {
        this.node = node;
        this.session = sparkSession;
    }

    @Override
    public void execute() throws Exception {
        List<String> dns = node.getDependNumber();
        String upTempView = dns.stream().filter(StringUtils::isNotBlank).findFirst().get();
        String tempView = node.getNumber();
        List<DataConvertDTO> rules = JSON.parseArray(JSON.toJSONString(node.getData()), DataConvertDTO.class);
        convertData(rules, upTempView, tempView);
    }

    private void convertData(List<DataConvertDTO> rules, String upTempView, String tempView) throws Exception {
        Dataset<Row> dataset = session.table(upTempView);
        if (dataset == null || dataset.count() <= 0) {
            logger.warn("数据转换节点的源数据记录为空：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            return;
        }
        if (rules == null || rules.isEmpty()) {
            logger.warn("数据转换节点配置的转换规则为空：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        List<String> columns = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<String> colNames = new ArrayList<>();
        List<Column> cols = new ArrayList<>();
        for (DataConvertDTO rule : rules) {
            if (rule == null || StringUtils.isBlank(rule.getCode()) || rule.getRules() == null || rule.getRules().isEmpty()) {
                logger.warn("数据转换节点配置的转换规则存在空记录：{}", JSON.toJSONString(node));
                continue;
            }
            String code = rule.getCode();
            if (!columns.contains(code)) {
                logger.warn("数据转换节点配置的转换字段 {} 在源数据中不存在：{}", code, JSON.toJSONString(node));
                continue;
            }
            String other = rule.getOtherValue();
            List<ConvertRule> crs = rule.getRules().stream().filter(it -> StringUtils.isNotBlank(it.getBefore())
                    && StringUtils.isNotBlank(it.getAfter())).collect(Collectors.toList());
            if (crs.isEmpty()){
                logger.warn("数据转换节点配置的转换规则存在空记录：{}", JSON.toJSONString(node));
                continue;
            }
            String asCode = code;
            if (rule.getIsCheck() != null && rule.getIsCheck()) {
                asCode = rule.getNewFieldCode();
            }
            if (StringUtils.isBlank(asCode)){
                logger.warn("数据转换节点配置的转换规则存储新字段名称为空：{}", JSON.toJSONString(node));
                continue;
            }
            Column column = functions.when(col(code).eqNullSafe(crs.get(0).getBefore()), crs.get(0).getAfter());
            for (int i = 1; i < crs.size(); i++) {
                column = column.when(col(code).eqNullSafe(crs.get(i).getBefore()), crs.get(i).getAfter());
            }
            column = column.otherwise(other).as(asCode);
            cols.add(column);
            colNames.add(asCode);
        }
        columns.stream().filter(it -> !colNames.contains(it)).forEach(c -> cols.add(col(c)));
        dataset = dataset.select((Seq) wrapRefArray(cols.toArray()));
        dataset.createOrReplaceTempView(tempView);
        session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
    }


 /*   public static void main(String[] args) {
        Map<String, Object> map = new HashMap<>();
        map.putAll(ImmutableMap.of("A", 1, "B", "3", "C", "3", "D", "2010-12-22"));
        Map<String, Object> map1 = new HashMap<>();
        map1.putAll(ImmutableMap.of("A", 2, "B", "2", "C", "3", "D", "2021-12-23"));
//        map1.put("D", null);
        Map<String, Object> map2 = new HashMap<>();
        map2.putAll(ImmutableMap.of("A", 3, "B", "1", "C", "3", "D", ""));
        Map<String, Object> map3 = new HashMap<>();
        map3.putAll(ImmutableMap.of("A", 4, "B", "3", "C", "3", "D", "2021-12-23"));
//        map3.put("D", null);
        Map<String, Object> map4 = new HashMap<>();
        map4.putAll(ImmutableMap.of("A", 5, "B", "2", "C", "3", "D", ""));
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
        dataset.show();
        List<org.apache.spark.sql.Column> ncols = new ArrayList<>();
        List<String> s = new ArrayList<>();
        s.add("经理");
        s.add("主管");
        s.add("我们的混子产品");
        s.add("研发");
        s.add("测试");
        String[] sr = {"A", "B", "C"};
        for (int i = 0; i < 3; i++) {
            Column column = functions.when(col(sr[i]).eqNullSafe(1), s.get(0));
            for (int j = 2; j < s.size(); j++) {
                column = column.when(col(sr[i]).eqNullSafe(j), s.get(j-1));
            }
            column = column.otherwise("混子").as(sr[i]);
            ncols.add(column);
        }
        dataset.select((Seq) wrapRefArray(ncols.toArray())).show();
        sparkSession.close();

    }*/
}
