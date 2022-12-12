package com.wk.data.spark.service.govern.executor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wk.data.etl.facade.govern.dto.TaskNodeDTO;
import com.wk.data.etl.facade.govern.dto.figure.FigureDTO;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * @Created: smash_hq at 15:57 2022/7/26
 * @Description: 数据计算
 */
public class DataFigureService extends Transform  implements Serializable, scala.Serializable {
    private static Logger logger = LoggerFactory.getLogger(DataFigureService.class);

    private TaskNodeDTO node;

    private SparkSession session;

    public DataFigureService(TaskNodeDTO node, SparkSession sparkSession) {
        this.node = node;
        this.session = sparkSession;
    }

    @Override
    public void execute() throws Exception {
        List<String> dns = node.getDependNumber();
        String upTempView = dns.stream().filter(StringUtils::isNotBlank).findFirst().get();
        String tempView = node.getNumber();
        List<FigureDTO> figureDTOS = JSONObject.parseArray(JSON.toJSONString(node.getData()), FigureDTO.class);
        figure(figureDTOS, upTempView, tempView);
    }

    private void figure(List<FigureDTO> figureDTOS, String upTempView, String tempView) {
        StringBuilder sqlSb = new StringBuilder();
        sqlSb.append("SELECT ");
        for (int i = 0, figureDTOSSize = figureDTOS.size(); i < figureDTOSSize; i++) {
            FigureDTO it = figureDTOS.get(i);
            Integer mode = it.getMode();
            String code = it.getCode();
            String detail = it.getDetail();
            String dateFormat = it.getDateFormat();
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            if (mode == 1) {
                sb.append(detail);
            } else {
                String str = detail;
                str = str.replace("'+'", "MARK&&MARK");
                str = str.replace("-", "MARK&&MARK");
                String[] strings = str.split("MARK&&MARK");
                for (int i1 = 0; i1 < strings.length; i1++) {
                    String temp = strings[i1];
                    String format = "unix_timestamp(" + temp + ")";
                    detail = detail.replace(temp, format);
                }
                sb.append(detail);
            }
            sb.append(") as ").append(code);
            sqlSb.append(sb);
            if (i < figureDTOS.size() - 1) {
                sqlSb.append(",");
            }
        }
        sqlSb.append(" FROM ").append(upTempView);
        Dataset<Row> sqlDataset = session.sql(sqlSb.toString());
        sqlDataset.show();
        sqlDataset.createOrReplaceTempView(tempView);
        session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
    }


//    public static void main(String[] args) {
//        String str = "[\n" +
//                "    {\n" +
//                "        \"mode\": 1,\n" +
//                "        \"code\": \"operation_1\",\n" +
//                "        \"detail\": \"process_definition_id*(process_definition_id-recovery)\"\n" +
//                "    },\n" +
//                "    {\n" +
//                "        \"mode\": 2,\n" +
//                "        \"code\": \"operation_2\",\n" +
//                "        \"detail\": \"end_time-start_time\"\n" +
//                "    }\n" +
//                "]";
//
//        List<FigureDTO> figureDTOS = JSONObject.parseArray(str, FigureDTO.class);
//        dataset();
//        figure(figureDTOS, "upTempView", "tempView");
//
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
//    private static void dataset() {
//        Dataset<Row> dataset = sparkSession().read().format("jdbc")
//                .option("url", "jdbc:mysql://10.50.125.141:3306/test_rule   ?useSSL=false&&characterEncoding=utf8")
//                .option("driver", "com.mysql.cj.jdbc.Driver")
//                .option("user", "root")
//                .option("password", "hxc@069.root_mysql")
//                .option("dbtable", "t_ds_process_instance")
//                .option("limit", "1000")
//                .load();
//        dataset.createOrReplaceTempView("upTempView");
//    }

}
