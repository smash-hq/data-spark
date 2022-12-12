package com.wk.data.spark.infrastructure.util;


import com.wk.data.etl.facade.standard.dto.RangeDTO;
import com.wk.data.spark.infrastructure.master.database.po.JoinRelationDO;
import com.wk.data.spark.infrastructure.master.database.po.SourceRelationDO;
import com.wk.data.spark.infrastructure.master.database.po.TableDO;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.lang.reflect.Array;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Author: smash_hq
 * @Date: 2021/11/18 14:17
 * @Description: 自定义函数测试
 * @Version v1.0
 */

public class TestFunction {

    public static SparkSession sparkSession() {
        String date = LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE);
        return SparkSession.builder()
                .appName("spark")
                .config("date", date)
                .config("spark.sql.debug.maxToStringFields", "100")
                .master("local[*]")
                .getOrCreate();
    }

    private static Dataset<Row> mergeFiled(Dataset<Row> dataset, Map<String, String> map, Column... columns) {

        return dataset.withColumn("concat", functions.concat_ws("--", columns).alias("concat"))
                .withColumn("concat_ws", functions.concat_ws("==", functions.col("concat"), functions.col("run_times")).alias("concat_ws"))
                .withColumn("end_time", functions.regexp_replace(functions.col("end_time"), "", ""))
                .drop("concat");
    }


    private static Dataset<Row> splitFiled(SparkSession spark, Dataset<Row> dataset) {
        try {
            dataset.createTempView("functions");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
        // udf方式
        DataType stringType = DataTypes.StringType;
        StructType rfSchema = new StructType(new StructField[]{
                new StructField("name1", stringType, true, Metadata.empty()),
                new StructField("name2", stringType, true, Metadata.empty()),
                new StructField("name3", stringType, true, Metadata.empty())});
        JavaRDD<Row> javaRdd = dataset.select(functions.col("name")).toJavaRDD().flatMap((FlatMapFunction<Row, Row>) row -> {
            // 需要写入的数据个数 j
            int j = rfSchema.length();
            List<Row> list = new ArrayList<>();
            Object[] obj1 = new String[j];
            Object[] obj2 = row.getAs("name").toString().split("-", j);
            // 复制数组，切分后数组长度不足的补救措施
            System.arraycopy(obj2, 0, obj1, 0, obj2.length);
            list.add(RowFactory.create(obj1));
            return list.iterator();
        });

        return spark.createDataFrame(javaRdd, rfSchema);
    }

    public static String sql(JoinRelationDO tables, List<SourceRelationDO> cfgs) {
        String left = tables.getLeftTable().getTableCode();

        StringBuilder filedCfgs = new StringBuilder();
        for (int i = 0; i < cfgs.size(); i++) {
            SourceRelationDO it = cfgs.get(i);
            String firstTable = it.getFirstTable();
            String firstFiled = it.getFirstFiled();
            String secondTable = it.getSecondTable();
            String secondFiled = it.getSecondFiled();
            String thirdTable = it.getThirdTable();
            String thirdFiled = it.getThirdFiled();
            String master = it.getMasterCode();
            if (StringUtils.isNotBlank(secondTable) && StringUtils.isNotBlank(secondFiled)) {
                filedCfgs.append("CASE WHEN ").append(firstTable).append(".").append(firstFiled)
                        .append(" IS NULL AND ").append(secondTable).append(".").append(secondFiled)
                        .append(" IS NOT NULL ").append(" THEN ").append(secondTable).append(".").append(secondFiled)
                        .append(StringUtils.isBlank(thirdTable) && StringUtils.isBlank(thirdFiled) ? "" :
                                new StringBuilder().append(" WHEN ").append(firstTable).append(".").append(firstFiled).append(" IS NULL AND ")
                                        .append(secondTable).append(".").append(secondFiled).append(" IS NULL THEN ")
                                        .append(thirdTable).append(".").append(thirdFiled).toString())
                        .append(" ELSE ").append(firstTable).append(".").append(firstFiled).append(" END ");
            } else {
                filedCfgs.append(firstTable).append(".").append(firstFiled);
            }
            filedCfgs.append(" AS ").append(master).append(i == cfgs.size() - 1 ? " " : ",");
        }

        String leftSql = "SELECT " + filedCfgs + " FROM " + left + " AS " + left;
        StringBuilder rightSql = new StringBuilder();
        for (TableDO tableDO : tables.getRightTables()) {
            String leftFiled = tableDO.getLeftFiled();
            String right = tableDO.getTableCode();
            String rightFiled = tableDO.getRightFiled();
            rightSql.append(" LEFT JOIN (").append("SELECT * FROM ").append(right).append(" AS ").append(right).append(")").append(right).append(" ON ")
                    .append(left).append(".").append(leftFiled).append("=")
                    .append(right).append(".").append(rightFiled);
        }

        return leftSql + rightSql;
    }


    public static void main(String[] args) {
        SparkSession spark = sparkSession();
        // 注册自定义函数

        group();

        spark.close();
    }

    private static void group() {
        Dataset<Row> dataset = sparkSession().read().format("jdbc")
                .option("url", "jdbc:mysql://10.50.125.141:3306/test_rule?useSSL=false&&characterEncoding=utf8")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("user", "root")
                .option("password", "hxc@069.root_mysql")
                .option("dbtable", "execution_flows")
                .load();
        RelationalGroupedDataset relationalGroupedDataset = dataset.groupBy();
        relationalGroupedDataset.max();

    }

    private static boolean rangeCheck(String value, List<RangeDTO> range) {
        List<String> r = range.stream().filter(it -> !isBlank(it.getRangeCode())
                || !isBlank(it.getRangeName())).map(RangeDTO::getRangeCode).collect(Collectors.toList());
        if (r.isEmpty()) {
            return true;
        }
        return r.contains(value);
    }

    private static boolean regularRule(String value, String checkRule) {
        if (checkRule == null || checkRule.length() == 0) {
            return true;
        }
        return value.matches(checkRule);
    }

    public static boolean isBlank(CharSequence cs) {
        int strLen = cs.length();
        if (strLen == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean exactValue(String t, Double minValue, Double maxValue) {
        if (maxValue == null && minValue == null) {
            return true;
        }
        double v;
        try {
            v = Double.parseDouble(t);
        } catch (NumberFormatException ex) {
            return false;
        }
        if (minValue != null && maxValue != null) {
            return v >= minValue && v <= maxValue;
        } else if (minValue != null) {
            return v >= minValue;
        } else {
            return v <= maxValue;
        }
    }


    public static boolean integrality(Object t, Boolean bool) {
        if (bool == null || !bool) {
            return false;
        }
        return !isEmpty(t);
    }

    private static boolean isEmpty(Object object) {
        if (object == null) {
            return true;
        }
        if (object instanceof CharSequence) {
            return ((CharSequence) object).length() == 0;
        }
        if (object.getClass().isArray()) {
            return Array.getLength(object) == 0;
        }
        if (object instanceof Collection<?>) {
            return ((Collection<?>) object).isEmpty();
        }
        if (object instanceof Map<?, ?>) {
            return ((Map<?, ?>) object).isEmpty();
        }
        return false;
    }

    public static boolean exactString(String t, Integer minString, Integer maxString) {
        if (minString == null && maxString == null) {
            return true;
        }
        int length = t.length();
        if (maxString != null && minString != null) {
            return maxString >= length && minString <= length;
        } else if (minString != null) {
            return minString <= length;
        } else {
            return maxString >= length;
        }
    }
}
