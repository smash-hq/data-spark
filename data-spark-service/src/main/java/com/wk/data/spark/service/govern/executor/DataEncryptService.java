package com.wk.data.spark.service.govern.executor;

import com.alibaba.fastjson.JSON;
import com.wk.data.etl.facade.govern.dto.TaskNodeDTO;
import com.wk.data.etl.facade.govern.dto.filter.DesensitiseDTO;
import com.wk.data.etl.facade.govern.enums.DesensitiseModeEnum;
import com.wk.data.etl.facade.govern.enums.DesensitiseTypeEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static scala.Predef.wrapRefArray;

/**
 * @program: data-spark-job
 * @description: 数据脱敏
 * @author: gwl
 * @create: 2021-12-10 15:33
 **/
public class DataEncryptService extends Transform implements Serializable, scala.Serializable {

    private static Logger logger = LoggerFactory.getLogger(DataEncryptService.class);

    private TaskNodeDTO node;

    private SparkSession session;

    public DataEncryptService(TaskNodeDTO node, SparkSession sparkSession) {
        this.node = node;
        this.session = sparkSession;
    }

    private static String icFormat = "idCardMask(cast(%s as string), %d)";

    @Override
    public void execute() throws Exception {
        List<String> dns = node.getDependNumber();
        String upTempView = dns.stream().filter(StringUtils::isNotBlank).findFirst().get();
        String tempView = node.getNumber();
        List<DesensitiseDTO> dss = JSON.parseArray(JSON.toJSONString(node.getData()), DesensitiseDTO.class);
        encryptData(dss, upTempView, tempView);
    }

    private void encryptData(List<DesensitiseDTO> dss, String upTempView, String tempView) throws Exception {
        Dataset<Row> dataset = session.table(upTempView);
        if (dataset.count() <= 0) {
            logger.warn("数据脱敏节点的源数据记录为空：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            return;
        }
        if (dss == null || dss.isEmpty()) {
            logger.warn("数据脱敏节点配置的脱敏规则为空：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        List<String> columns = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<String> colNames = new ArrayList<>();
        List<Column> cols = new ArrayList<>();
        for (DesensitiseDTO dt : dss) {
            if (dt == null || StringUtils.isBlank(dt.getCode()) || StringUtils.isBlank(dt.getType())
                    || StringUtils.isBlank(dt.getMode()) || colNames.contains(dt.getCode())
                    || !columns.contains(dt.getCode())) {
                continue;
            }
            DesensitiseTypeEnum type = DesensitiseTypeEnum.getEnumByEnName(dt.getType());
            String code = dt.getCode();
            switch (type) {
                case NAME:
                    colNames.add(code);
                    cols.add(functions.callUDF("nameMask", dataset.col(code).cast(DataTypes.StringType)).as(code));
                    break;
                case IDCARD:
                    DesensitiseModeEnum mode = DesensitiseModeEnum.getEnumByEnName(dt.getMode());
                    int m = mode == null || DesensitiseModeEnum.IDCARD2.equals(mode)? 2 : 1;
                    colNames.add(code);
                    cols.add(functions.expr(String.format(icFormat, code, m)).as(code));
                    break;
                case PTHONE:
                    colNames.add(code);
                    cols.add(functions.callUDF("phoneMask", dataset.col(code).cast(DataTypes.StringType)).as(code));
                    break;
                default:
                    break;
            }
        }
        if (colNames.isEmpty()) {
            logger.warn("数据脱敏节点配置的过滤字段跟源数据字段不匹配：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        columns.stream().filter(it -> !colNames.contains(it)).forEach(c -> cols.add(col(c)));
        dataset = dataset.select((Seq) wrapRefArray(cols.toArray()));
        dataset.createOrReplaceTempView(tempView);
        session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
    }

   /*  public static void main(String[] args) {
        Map<String, Object> map = new HashMap<>();
        map.putAll(ImmutableMap.of("A", 1, "B", "张三", "C", 15201115858L, "D", "620502199908056587"));
        Map<String, Object> map1 = new HashMap<>();
        map1.putAll(ImmutableMap.of("A", 1, "B", "李四", "C", 15201115858L, "D", "620502199908056587"));
        Map<String, Object> map2 = new HashMap<>();
        map2.putAll(ImmutableMap.of("A", 2, "B", "王五", "C", 15201115858L, "D", "620502199908056587"));
        Map<String, Object> map3 = new HashMap<>();
        map3.putAll(ImmutableMap.of("A", 2, "B", "诸葛孔明", "C", 15201115858L, "D", "620502199908056587"));
        Map<String, Object> map4 = new HashMap<>();
        map4.putAll(ImmutableMap.of("A", 2, "B", "欧阳娜娜", "C", 15201115858L, "D", "620502199908056587"));
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
         sparkSession.sqlContext().udf().register("nameMask", new NameMaskUdf(), DataTypes.StringType);
         sparkSession.sqlContext().udf().register("phoneMask", new PhoneNumberMaskUdf(), DataTypes.StringType);
         sparkSession.sqlContext().udf().register("idCardMask", new IdCardMaskUdf(), DataTypes.StringType);
         // 注册成表
        Dataset<Row> dataset = sparkSession.read().json(javaRDD);
        dataset.show();
        List<String> name = new ArrayList<>();
         name.add("B");
         name.add("C");
         name.add("D");
         List<Column> cols = new ArrayList<>();
         cols.add(functions.expr("nameMask(cast(B as string))"));
         cols.add(functions.expr("phoneMask(cast(C as string))"));
         cols.add(functions.expr("idCardMask(cast(D as string), 1)"));
        dataset = dataset.select(functions.callUDF("nameMaskUDF", dataset.col("B").cast(DataTypes.StringType)));
       *//* dataset = dataset.withColumns(JavaConverters.asScalaIteratorConverter(name.iterator()).asScala().toSeq(),
                JavaConverters.asScalaIteratorConverter(cols.iterator()).asScala().toSeq());*//*
        dataset.show();
          *//*      select(col("A"), col("B"), col("C"),
                col("D").cast(DataTypes.TimestampType))
                .withColumn("S", functions.callUDF("getLength", dataset.col("C").cast( DataTypes.StringType)));
        sparkSession.sqlContext().udf().register("getLength", getLength(), DataTypes.IntegerType);
        dataset = dataset.orderBy(col("S").asc()).dropDuplicates("A");
        dataset.show();
        dataset.createOrReplaceTempView("cd");
        sparkSession.catalog().cacheTable("cd");
        sparkSession.table("ab").show();
        sparkSession.table("cd").show();*//*
        sparkSession.close();

    }*/
}
