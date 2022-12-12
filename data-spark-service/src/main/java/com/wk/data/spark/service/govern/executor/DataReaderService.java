package com.wk.data.spark.service.govern.executor;

import com.alibaba.fastjson.JSON;
import com.wk.data.etl.facade.govern.dto.TaskNodeDTO;
import com.wk.data.etl.facade.govern.dto.input.DataInputDTO;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static scala.Predef.wrapRefArray;

/**
 * @program: data-spark-job
 * @description: 数据加载
 * @author: gwl
 * @create: 2021-12-10 15:33
 **/
@Component
public class DataReaderService implements Serializable, scala.Serializable {

    private static Logger logger = LoggerFactory.getLogger(DataReaderService.class);

    @Autowired
    private SparkSession session;

    @Value("${mongodb.data-lake}")
    private String lakeUrl;
    @Value("${mysql.url}")
    private String url;
    @Value("${mysql.driver}")
    private String driver;
    @Value("${mysql.username}")
    private String username;
    @Value("${mysql.password}")
    private String password;

    public List<String> loadDataSet(List<TaskNodeDTO> inputs) throws Exception {
        List<String> list = new ArrayList<>();
        for (TaskNodeDTO dto : inputs) {
            Object obj = dto.getData();
            DataInputDTO input = JSON.parseObject(JSON.toJSONString(obj), DataInputDTO.class);
            String tempView = dto.getNumber();
            Dataset<Row> dataset;
            if ("SDI".equals(input.getDataLayer())) {
                dataset = session.read().format("com.mongodb.spark.sql")
                        .option("uri", lakeUrl)
                        .option("collection", input.getTableCode())
//                        .option("pipeline", "[{ $match: { ruleSerial:  \"RULE_bV9w89rv2\"} }]")
                        .load();
            } else {
                dataset = session.read().format("jdbc")
                        .option("url", String.format(url, input.getDbCode()))
                        .option("driver", driver)
                        .option("user", username)
                        .option("password", password)
                        .option("dbTable", input.getTableCode())
                        .load();
            }
            if (dataset.count() <= 0) {
                logger.warn("数据输入节点的源数据记录为空：{}", JSON.toJSONString(dto));
            }
            List<com.wk.data.etl.facade.govern.dto.clean.Column> inputCols = input.getCols();
            List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
            List<Column> ncols = new ArrayList<>();
            if (inputCols != null && !inputCols.isEmpty()) {
                inputCols.stream().filter(it -> it != null && StringUtils.isNotBlank(it.getCode())).
                        forEach(d ->
                                ncols.add(cols.contains(d.getCode()) ? col(d.getCode()) : lit(null).as(d.getCode()))
                        );
            }
            if (ncols.isEmpty()) {
                logger.warn("数据输入节点的源数据所选字段为空：{}", JSON.toJSONString(dto));
            } else {
                dataset = dataset.select((Seq) wrapRefArray(ncols.toArray()));
            }
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            list.add(tempView);
        }
        return list;
    }

}
