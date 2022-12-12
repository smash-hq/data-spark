package com.wk.data.spark.service.govern.executor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wk.data.etl.facade.govern.dto.TaskNodeDTO;
import com.wk.data.etl.facade.govern.dto.convert.ConditionConvertDTO;
import com.wk.data.etl.facade.govern.dto.convert.ConditionDTO;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * @Created: smash_hq at 16:17 2022/7/26
 * @Description: 条件转换
 */

public class DataConvertByConditionService extends Transform implements Serializable, scala.Serializable {
    private static Logger logger = LoggerFactory.getLogger(DataConvertByConditionService.class);

    private TaskNodeDTO node;

    private SparkSession session;

    public DataConvertByConditionService(TaskNodeDTO node, SparkSession sparkSession) {
        this.node = node;
        this.session = sparkSession;
    }

    @Override
    public void execute() throws Exception {
        List<String> dns = node.getDependNumber();
        String upTempView = dns.stream().filter(StringUtils::isNotBlank).findFirst().get();
        String tempView = node.getNumber();
        List<ConditionConvertDTO> condition = JSONObject.parseArray(JSON.toJSONString(node.getData()), ConditionConvertDTO.class);
        convert(condition, upTempView, tempView);
    }

    private void convert(List<ConditionConvertDTO> condition, String upTempView, String tempView) {
        Dataset<Row> dataset = session.table(upTempView);
        for (ConditionConvertDTO it : condition) {
            Integer mode = it.getMode();
            String code = it.getCode();
            String otherValue = it.getOtherValue();
            String toCode = it.getToCode();
            List<ConditionDTO> list = it.getList();
            String rule = JSONObject.toJSONString(list);
            if (mode == 1) {
                dataset = dataset.withColumn(toCode, functions.expr("valueConvert(" + code + ",'" + rule + "','" + otherValue + "')"));
            }
            if (mode == 2) {
                dataset = dataset.withColumn(toCode, functions.expr("rangeConvert(" + code + ",'" + rule + "','" + otherValue + "')"));
            }
        }
        dataset.createOrReplaceTempView(tempView);
//        writer(dataset);
        session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());

    }

//    public static void main(String[] args) {
//        String str = "[\n" +
//                "    {\n" +
//                "        \"mode\": 1,\n" +
//                "        \"code\": \"exec_id\",\n" +
//                "        \"otherValue\": \"otherValue\",\n" +
//                "        \"toCode\": \"exec_id_to_code\",\n" +
//                "        \"list\":\n" +
//                "        [\n" +
//                "            {\n" +
//                "                \"source\": \"1;1000\",\n" +
//                "                \"target\": \"1-1000\"\n" +
//                "            },\n" +
//                "            {\n" +
//                "                \"source\": \"1001;2000\",\n" +
//                "                \"target\": \"1001-2000\"\n" +
//                "            },\n" +
//                "            {\n" +
//                "                \"source\": \"2001;3000\",\n" +
//                "                \"target\": \"2001-3000\"\n" +
//                "            },\n" +
//                "            {\n" +
//                "                \"source\": \"3001;4000\",\n" +
//                "                \"target\": \"3001-4000\"\n" +
//                "            }\n" +
//                "        ]\n" +
//                "    },\n" +
//                "    {\n" +
//                "        \"mode\": 2,\n" +
//                "        \"code\": \"project_id\",\n" +
//                "        \"otherValue\": \"otherValue\",\n" +
//                "        \"toCode\": \"project_id_to_code\",\n" +
//                "        \"list\":\n" +
//                "        [\n" +
//                "            {\n" +
//                "                \"source\": \"1;2;3\",\n" +
//                "                \"target\": \"123\"\n" +
//                "            },\n" +
//                "            {\n" +
//                "                \"source\": \"4;5;6\",\n" +
//                "                \"target\": \"456\"\n" +
//                "            },\n" +
//                "            {\n" +
//                "                \"source\": \"29;92\",\n" +
//                "                \"target\": \"2992\"\n" +
//                "            }\n" +
//                "        ]\n" +
//                "    }\n" +
//                "]";
//        sparkSession().sqlContext().udf().register("valueConvert", new ValueConvertUdf<>(), DataTypes.StringType);
//        sparkSession().sqlContext().udf().register("rangeConvert", new RangeConvertUdf<>(), DataTypes.StringType);
//        List<ConditionConvertDTO> conditionConvertDTOS = JSONObject.parseArray(str, ConditionConvertDTO.class);
//        reader();
//        convert(conditionConvertDTOS, "upTempView", "downTempView");
//    }
//
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
//    private static void reader() {
//        Dataset<Row> dataset = sparkSession().read().format("jdbc")
//                .option("url", "jdbc:mysql://10.50.125.141:3306/test_rule   ?useSSL=false&&characterEncoding=utf8")
//                .option("driver", "com.mysql.cj.jdbc.Driver")
//                .option("user", "root")
//                .option("password", "hxc@069.root_mysql")
//                .option("dbtable", "execution_flows")
//                .load();
//        dataset.createOrReplaceTempView("upTempView");
//    }
//
//    private static void writer(Dataset<Row> dataset) {
//        dataset.write().mode(SaveMode.Overwrite).format("com.mongodb.spark.sql")
//                .option("uri", "mongodb://root:123456@10.50.125.142:27017/data-etl?authSource=admin&authMechanism=SCRAM-SHA-256")
//                .option("database", "detection")
//                .option("collection", "analysis_test_table")
//                .option("batchSize", 10000)
//                .option("isolationLevel", "NONE")
//                .save();
//    }


}
