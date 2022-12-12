package com.wk.data.spark.service.govern.executor;

import com.alibaba.fastjson.JSON;
import com.vcolco.components.basis.exception.BizException;
import com.vcolco.components.basis.result.MultiResult;
import com.vcolco.components.basis.result.SingleResult;
import com.wk.data.etl.facade.catalog.api.CatalogFacade;
import com.wk.data.etl.facade.catalog.vo.CatalogVO;
import com.wk.data.etl.facade.govern.dto.TaskNodeDTO;
import com.wk.data.etl.facade.govern.dto.output.DataOutputDTO;
import com.wk.data.etl.facade.govern.dto.output.OutputColumnDTO;
import com.wk.data.etl.facade.govern.dto.output.SourceField;
import com.wk.data.etl.facade.govern.enums.TaskNodeEnum;
import com.wk.data.etl.facade.standard.api.StandardFacade;
import com.wk.data.etl.facade.standard.dto.StandardDTO;
import com.wk.data.etl.facade.table.ColInfo;
import com.wk.data.share.facade.subscribe.api.DataSubscribeFacade;
import com.wk.data.share.facade.subscribe.dto.CatalogInfoDTO;
import com.wk.data.spark.infrastructure.coon.ConnectionPool;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.collection.Seq;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.wk.data.etl.facade.govern.enums.TaskNodeEnum.DATA_OUTPUT;
import static com.wk.data.spark.infrastructure.util.General.ALIAS_FORMAT;
import static org.apache.spark.sql.functions.col;
import static scala.Predef.wrapRefArray;

/**
 * @program: data-spark-job
 * @description: 数据写入
 * @author: gwl
 * @create: 2021-12-10 15:33
 **/
@Component
public class DataWriterService implements Serializable, scala.Serializable {

    private static Logger logger = LoggerFactory.getLogger(DataWriterService.class);


    @Autowired
    private SparkSession session;

    @Value("${mysql.url}")
    private String url;
    @Value("${mysql.driver}")
    private String driver;
    @Value("${mysql.username}")
    private String username;
    @Value("${mysql.password}")
    private String password;

    @Autowired
    private CatalogFacade catalogFacade;
    @Autowired
    private StandardFacade standardFacade;
    @Autowired
    private DataSubscribeFacade dataSubscribeFacade;

    public void writeData(List<TaskNodeDTO> outputs) throws Exception {
        for (TaskNodeDTO node : outputs) {
            Object obj = node.getData();
            String jstr = JSON.toJSONString(obj);
            DataOutputDTO output = JSON.parseObject(jstr, DataOutputDTO.class);
            List<String> dpns = node.getDependNumber();
            if (dpns == null || dpns.isEmpty() || StringUtils.isBlank(dpns.get(0))) {
                throw new BizException("数据输出节点没有数据来源：" + node.getName());
            }
            TaskNodeEnum taskNode = TaskNodeEnum.getEnumByEnName(node.getType());
            excuteWrite(output, dpns.get(0), taskNode);
        }
    }

    private void excuteWrite(DataOutputDTO output, String table, TaskNodeEnum taskNode) throws Exception {
        String msg = output.getType() == 0 ? "输出" : "集成输出";
        List<OutputColumnDTO> outputCols = output.getCols();
        if (outputCols == null || outputCols.isEmpty()) {
            logger.error("数据{}节点的输出字段为空:{}", msg, output);
            return;
        }
        Dataset<Row> dataset = session.table(table);
        if (dataset.count() <= 0) {
            logger.warn("数据{}节点的数据为空:{}", msg, output);
            return;
        }
        List<ColInfo> colList = getCatalogColInfo(output.getTableId());
        // 重置输入字段的类型
        outputCols = upsetFieldType(outputCols, colList);
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<Column> list = parseOutputColumn(cols, outputCols, taskNode);
        if (list.isEmpty()) {
            logger.error("数据{}配置的字段跟源数据不匹配: 输出字段：{}， 源字段：{}", msg, outputCols, Arrays.toString(list.toArray()));
            return;
        }
        dataset = dataset.select((Seq) wrapRefArray(list.toArray()));
        // todo 2、先根据数据标准字段进行数据过滤
        dataset = filterData(dataset, colList);
        long count = dataset.count();
        int scope = output.getScope();
        if (scope == 2) { // 若是更新且主键不为空,则删除重复记录，通过主键删除
            // 若存在主键字段，则进行删除，若不存在，则不删除
            dropDuplicates(dataset, colList, count, output);
        }
        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        long writeTotal = dataset.count();
        logger.warn("数据治理基本流程已完成，准备写入存储数据库：{} >> 表：{}，写入数据条数  {} 条", output.getDbCode(), output.getTableCode(), writeTotal);
        if (scope == 0) {
//            cleanTable(output);
            dataset.write()
                    .mode(SaveMode.Overwrite)
                    .format("jdbc")
                    .option("url", String.format(url, output.getDbCode()))
                    .option("dbTable", output.getTableCode())
                    .option("user", username)
                    .option("password", password)
                    .option("driver", driver)
                    .option("truncate", true)
                    .option("batchSize", 10000)
                    .option("isolationLevel", "NONE")
                    .save();
        } else {
            dataset.write()
                    .mode(SaveMode.Append)
                    .format("jdbc")
                    .option("url", String.format(url, output.getDbCode()))
                    .option("dbTable", output.getTableCode())
                    .option("user", username)
                    .option("password", password)
                    .option("driver", driver)
                    .option("batchSize", 10000)
                    .option("isolationLevel", "NONE")
                    .save();
        }
        logger.warn("成功写入数据库-{} 中的表-{} 的数据为  {} 条", output.getDbCode(), output.getTableCode(), writeTotal);
        CatalogInfoDTO catalogInfo = CatalogInfoDTO.builder().layer(output.getDataLayer()).date(date)
                .dbCode(output.getDbCode()).tableCode(output.getTableCode()).type(output.getScope()).build();
        dataSubscribeFacade.sendDataChangeInfo(catalogInfo);
    }

    private List<Column> parseOutputColumn(List<String> cols, List<OutputColumnDTO> list, TaskNodeEnum taskNode) {
        List<Column> columns = new ArrayList<>();
        switch (taskNode) {
            case DATA_OUTPUT:
                list.stream().filter(it ->
                        StringUtils.isNotBlank(it.getCode()) &&
                                StringUtils.isNotBlank(it.getSourceCode()) &&
                                StringUtils.isNotBlank(it.getType())
                        )
                        .forEach(o -> {
                            if (cols.contains(o.getSourceCode())) {
                                columns.add("json".equals(o.getType()) ?
                                        functions.to_json(col(o.getSourceCode())).as(o.getCode()) :
                                        col(o.getSourceCode()).cast(getDataType(o.getType())).as(o.getCode()));
                            }
                        });
                break;
            case INTEGRATE_OUTPUT:
                for (OutputColumnDTO ocd : list) {
                     List<String> sfs = buildField(ocd, cols);
                     if (sfs.isEmpty()) {
                         continue;
                     } else if (sfs.size() == 1) {
                         columns.add(col(sfs.get(0)).cast(getDataType(ocd.getType())).as(ocd.getCode()));
                     } else {
                         Column c = functions.when(col(sfs.get(0)).isNotNull(), col(sfs.get(0)));
                         int len = sfs.size() - 1;
                         for (int i = 1; i <= len; i++) {
                             if (i == len) {
                                 c = c.otherwise(col(sfs.get(i))).cast(getDataType(ocd.getType())).as(ocd.getCode());
                             } else {
                                 c = c.when(col(sfs.get(i)).isNotNull(), col(sfs.get(i)));
                             }
                         }
                         columns.add(c);
                     }
                }
                break;
            default:
                return columns;
        }
        return columns;
    }

    private List<String> buildField(OutputColumnDTO dto, List<String> cols) {
        List<SourceField> sfs = new ArrayList<>();
        if (dto.getFirst() != null) {
            sfs.add(dto.getFirst());
        }
        if (dto.getSecond() != null) {
            sfs.add(dto.getSecond());
        }
        if (dto.getThird() != null) {
            sfs.add(dto.getThird());
        }
        return sfs.stream()
                .filter(it -> StringUtils.isNotBlank(it.getNickname()) && StringUtils.isNotBlank(it.getField()))
                .map(o -> String.format(ALIAS_FORMAT, o.getNickname(), o.getField()))
                .filter(s -> cols.contains(s))
                .collect(Collectors.toList());
    }

    private void dropDuplicates(Dataset<Row> dataset, List<ColInfo> colList, long count, DataOutputDTO output) throws Exception {
        Map<String, String> map = colList.stream().filter(ColInfo::getIsPrimaryKey).collect(Collectors.toMap(ColInfo::getCode, ColInfo::getType));
        if (map == null || map.isEmpty()) {
            logger.warn("输入节点的编目表不存在主键，数据更新模式变为追加写入");
            return;
        }
        String dbcode = output.getDbCode();
        String tableName = output.getTableCode();
        batchDeleteByPk(map, dataset, dbcode, tableName, count);
    }

    public void batchDeleteByPk(Map<String, String> map, Dataset<Row> dataset, String dbcode, String tableName, long count) throws Exception {
        Connection connection = ConnectionPool.getConnection(url + dbcode, username, password, driver);
        connection.setAutoCommit(false);
        try {
            List<String> keys = new ArrayList<>(map.keySet());
            int keysSize = keys.size();
            if (keysSize == 1) {
                String key = keys.get(0);
                Dataset<Row> ds = dataset.select(col(key));
                for (int i = 0; (i * 500) < count; i++) {
                    Dataset<Row> ds1 = ds.limit(500);
                    List<String> vals = ds1.collectAsList().stream()
                            .map(row -> row.getString(row.fieldIndex(key))).collect(Collectors.toList());
                    ds = ds.except(ds1);
                    StringBuilder sb = new StringBuilder().append("DELETE FROM ").append(tableName).append(" WHERE ")
                            .append(key).append(" IN (");
                    sb.append("'").append(StringUtils.join(vals, "','")).append("'");
                    sb.append(")");
                    connection.prepareStatement(sb.toString()).execute();
                }
            } else {
                dataset.foreach(row -> {
                    StringBuilder sb = new StringBuilder().append("DELETE FROM ").append(tableName).append(" WHERE ");
                    for (int i = 0; i < keysSize; i++) {
                        sb.append(keys.get(i)).append("='").append(row.getString(row.fieldIndex(keys.get(i)))).append("'");
                        if (i == keysSize - 1) {
                            break;
                        } else {
                            sb.append(" AND ");
                        }
                    }
                    connection.prepareStatement(sb.toString()).execute();
                });
            }
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.close();
        }
    }

    private List<OutputColumnDTO> upsetFieldType(List<OutputColumnDTO> outputCols, List<ColInfo> cols) {
        Map<String, String> map = cols.stream().collect(Collectors.toMap(ColInfo::getCode, ColInfo::getType));
        outputCols.stream().forEach(it -> it.setType(map.get(it.getCode())));
        return outputCols;
    }

    private DataType getDataType(String type) {
        switch (type) {
            case "integer":
                return DataTypes.IntegerType;
            case "decimal":
                return DataTypes.DoubleType;
            case "date":
                return DataTypes.DateType;
            case "timestamp":
                return DataTypes.TimestampType;
            case "tinyint":
                return DataTypes.ShortType;
            case "bigint":
                return DataTypes.LongType;
            default:
                return DataTypes.StringType;
        }
    }

    private Dataset<Row> filterData(Dataset<Row> dataset, List<ColInfo> cols) {
        List<String> list = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<String> nncodes = cols.stream().filter(ColInfo::getIsNotNull).map(ColInfo::getCode).collect(Collectors.toList());
        Column where = null;
        if (!nncodes.isEmpty()) {
            if (!list.containsAll(nncodes)) {
                logger.error("数据输入节点 配置的输入表中的非空字段 :{} 在实际输入数据中找不到: {}", Arrays.toString(nncodes.toArray()),
                        Arrays.toString(list.toArray()));
                throw new BizException("数据输入节点 配置的输入表中的非空字段跟实际数据不匹配");
            }
            int len = nncodes.size();
            where = col(nncodes.get(0)).isNotNull();
            for (int i = 1; i < len; i++) {
                where = where.and(col(nncodes.get(i)).isNotNull());

            }
        }
        where = concatStandWhere(cols, list, where);
        if (where != null) {
            dataset = dataset.where(where);
            long count = dataset.count();
            if (count <= 0) {
                logger.error("数据输出节点 没有满足存储表对应标准的数据:{}", where);
                throw new BizException("数据输出节点 没有满足存储表对应标准的数据");
            }
        }
        return dataset;

    }

    private Column concatStandWhere(List<ColInfo> cols, List<String> list, Column where) {
        Map<String, String> map = cols.stream().filter(it -> StringUtils.isNotBlank(it.getStandardId()))
                .collect(Collectors.toMap(ColInfo::getStandardId, ColInfo::getCode));
        List<StandardDTO> sts = getStandarfInfos(map, list);
        if (sts == null || sts.isEmpty()) {
            return where;
        }
        int len = sts.size();
        int i = 0;
        if (where == null) {
            where = rangeWhere(map, sts.get(0));
//            where = col(map.get(sts.get(0).getId())).isin(getRangeList(sts.get(0)));
            i = 1;
        }
        for (int j = i; j < len; j++) {
            where = where.and(rangeWhere(map, sts.get(j)));
//            where = where.and(col(map.get(sts.get(j).getId())).isin(getRangeList(sts.get(j))));
        }
        return where;
    }

    private List<StandardDTO> getStandarfInfos(Map<String, String> map, List<String> list) {
        if (map.isEmpty()) {
            return null;
        }
        MultiResult<StandardDTO> result;
        try {
            result = standardFacade.getBatch(new ArrayList<>(map.keySet()));
        } catch (Exception e) {
            logger.error("standardFacade.getBatch({})批量查询数据标准详情无失败", map.keySet(), e);
            throw e;
        }
        if (!result.isSuccess() || result.getData() == null || result.getData().isEmpty()) {
            logger.error("standardFacade.getBatch({})批量查询数据标准详情无数据：{}", map.keySet(), result);
        }
        List<StandardDTO> cols = result.getData();
        if (cols != null && !cols.isEmpty()) {
            cols = cols.stream().filter(it -> list.contains(map.get(it.getId())) && it.getRange() != null
                    && !it.getRange().isEmpty()).collect(Collectors.toList());
        }
        if (cols == null || cols.isEmpty()) {
            logger.warn("standardFacade.getBatch()批量查询数据标准详情中没有值域或对应字段，参数：{}， 结果：{}", map.keySet(), result);
        }
        return cols;
    }

    private Column rangeWhere(Map<String, String> map, StandardDTO standardDTO) {
        List<String> valeus = new ArrayList<>();
        standardDTO.getRange().stream().filter(it -> it != null && StringUtils.isNotBlank(it.getRangeCode()))
                .forEach(d -> valeus.add(d.getRangeCode()));
        String name = map.get(standardDTO.getId());
        if (standardDTO.getIsNotNull()) {
            return col(name).isin(wrapRefArray(valeus.toArray()));
        }
        return col(name).isNull().or(col(name).isin(wrapRefArray(valeus.toArray())));

    }


    /**
     * 通过编目表ID查询配置详情
     *
     * @param tableId
     * @return
     */
    private List<ColInfo> getCatalogColInfo(String tableId) {
        SingleResult<CatalogVO> result;
        try {
            result = catalogFacade.getSingle(tableId);
        } catch (Exception e) {
            logger.error("CatalogFacade.getSingle({})查询编目详情无失败", tableId, e);
            throw e;
        }
        if (!result.isSuccess() || result.getData() == null) {
            logger.error("CatalogFacade.getSingle({})查询编目详情无数据：{}", tableId, result);
            throw new BizException("查询编目详情数据返回空");
        }
        List<ColInfo> cols = result.getData().getColInfo();
        if (cols != null && !cols.isEmpty()) {
            cols = cols.stream().filter(it -> it != null &&
                    StringUtils.isNotBlank(it.getCode()) &&
                    !"id".equals(it.getCode())
            ).collect(Collectors.toList());
        }
        if (cols == null || cols.isEmpty()) {
            logger.error("CatalogFacade.getSingle({})查询编目详情数据详情中没有对应的字段列表：{}", tableId, result);
            throw new BizException("查询编目详情数据返回空");
        }
        return cols;
    }

   /* public static void main(String[] args) throws Exception {
        SparkSession sparkSession = SparkSession.builder()
                .config("date", LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE))
                .config("spark.sql.debug.maxToStringFields", "100")
                .master("local[*]")
                .getOrCreate();
        JavaRDD<String> javaRDD1 = new JavaSparkContext(sparkSession.sparkContext()).parallelize(initList(new String[]{"id", "name", "age"}, 11));
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
        Column column = functions.when(col("t3^id").isNotNull(), col("t3^id")).when(col("t2^id").isNotNull(), col("t2^id")).otherwise(col("t1^id")).as("id");
        dataset = dataset.select(column);
        dataset.show();
        sparkSession.close();
    }

    private static List<String> initList(String[] keys, int size) {
        Random random = new Random();
        List<String> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Map<String, Object> map = new HashMap<>();
            for (int j = 0; j < keys.length; j++) {
                map.put(keys[j], randomObjectValue(j, random, size));
            }
            list.add(JSON.toJSONString(map));
        }
        return list;
    }

    private static Object randomObjectValue(int j, Random random, int max) {
        switch (j) {
            case 0:
            case 4:
                return random.nextInt(max);
            case 5:
                return LocalTime.now();
            default:
                return UUID.randomUUID().toString();
        }
    }*/

    /*public static void main(String[] args) throws Exception {
        SparkSession sparkSession = SparkSession.builder()
                .config("date", LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE))
                .config("spark.sql.debug.maxToStringFields", "100")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> dataset1 = sparkSession.read().format("jdbc")
                .option("url", "jdbc:mysql://10.50.125.141:3306/azkaban?useSSL=false&&characterEncoding=utf8")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("user", "root")
                .option("password", "hxc@069.root_mysql")
                .option("dbTable", "projects")
                .load();
        dataset1.createOrReplaceTempView("table1");
        Dataset<Row> dataset2 = sparkSession.read().format("jdbc")
                .option("url", "jdbc:mysql://10.50.125.141:3306/azkaban?useSSL=false&&characterEncoding=utf8")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("user", "root")
                .option("password", "hxc@069.root_mysql")
                .option("dbTable", "project_versions")
                .load();
        dataset2.createOrReplaceTempView("table2");
        Dataset<Row> dataset3 = sparkSession.read().format("jdbc")
                .option("url", "jdbc:mysql://10.50.125.141:3306/azkaban?useSSL=false&&characterEncoding=utf8")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("user", "root")
                .option("password", "hxc@069.root_mysql")
                .option("dbTable", "project_flows")
                .load();
        dataset3.createOrReplaceTempView("table3");
        List<JoinTable> joins = new ArrayList<>();
        joins.add(JoinTable.builder().preNumber("table2").nickname("t2").leftField("id").rightField("project_id").build());
        joins.add(JoinTable.builder().preNumber("table3").nickname("t3").leftField("id").rightField("project_id").build());
        DataIntegrate integrate = DataIntegrate.builder().preNumber("table1").nickname("t1").joins(joins).build();
        TaskNodeDTO node = TaskNodeDTO.builder().number("union").data(integrate).build();
        DataJoinService join = new DataJoinService(node, sparkSession);
        join.executeDataJoin();
        Dataset<Row> dataset = sparkSession.table("union");
        dataset.show();
        DataWriterService writerService = new DataWriterService();
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<OutputColumnDTO> outputCols = new ArrayList<>();
        outputCols.add(OutputColumnDTO.builder().code("pid").type("integer")
                .first(SourceField.builder().field("id").nickname("t1").build())
                .second(SourceField.builder().field("project_id").nickname("t2").build())
                .third(SourceField.builder().field("project_id").nickname("t3").build()).build());
        outputCols.add(OutputColumnDTO.builder().code("name").type("varchar")
                .first(SourceField.builder().field("name").nickname("t1").build())
                .build());
        outputCols.add(OutputColumnDTO.builder().code("version").type("integer")
                .first(SourceField.builder().field("version").nickname("t2").build())
                .second(SourceField.builder().field("version").nickname("t1").build())
                .third(SourceField.builder().field("version").nickname("t3").build()).build());
        outputCols.add(OutputColumnDTO.builder().code("file_name").type("varchar")
                .first(SourceField.builder().field("varchar").nickname("t2").build())
                .build());
        outputCols.add(OutputColumnDTO.builder().code("flow_id").type("varchar")
                .first(SourceField.builder().field("flow_id").nickname("t3").build())
                .build());
        List<Column> list = writerService.parseOutputColumn(cols, outputCols, TaskNodeEnum.INTEGRATE_OUTPUT);
        if (list.isEmpty()) {
            logger.error("数据{}配置的字段跟源数据不匹配: 输出字段：{}， 源字段：{}", "shuchu ", outputCols, Arrays.toString(list.toArray()));
        }
        dataset = dataset.select((Seq) wrapRefArray(list.toArray()));
        dataset.show();

    }*/

    /*public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .config("date", LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE))
                .config("spark.sql.debug.maxToStringFields", "100")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> dataset = sparkSession.read().format("com.mongodb.spark.sql")
                .option("uri", "mongodb://root:123456@10.50.125.142:27017/resource-center?authSource=admin&authMechanism=SCRAM-SHA-256")
                .option("collection", "business_data_vehicle")
                .load();

        dataset = dataset.select(col("_id").cast(DataTypes.StringType).as("id"), col("annualExaminationsResult"),
                col("bargeEndDate").cast(DataTypes.DateType), col("bargeStartDate").cast(DataTypes.DateType),
                col("brand"),col("brandModel"),col("businessArea"),col("businessScope"),col("businessScopeCode"),
                col("callerId"),col("cameraNumber"),col("carAddressId"),col("carAddressName"),col("coachType"),
                col("completeMaintenanceDate"),col("createBy"),col("createName"),col("createTime").cast(DataTypes.DateType),col("deleted"),
                functions.to_json(col("credentialsInfo")).as("credentialsInfo"),col("doubleDrive"),col("endPlace"),
                col("endStation"),col("engineNumber"),col("enginePower"),col("enterprise"),col("enterpriseId")
                ,col("fuelTypeCode"),col("gmtAnnualExaminations").cast(DataTypes.DateType),
                col("gmtAnnualExaminationsValidityPeriod").cast(DataTypes.DateType),col("gmtCoachLevelExpiry").cast(DataTypes.DateType)
                ,col("gmtManufacture").cast(DataTypes.DateType),col("gmtRegistration").cast(DataTypes.DateType),
                col("gmtShortCre"),col("gmtTechLevelJudge").cast(DataTypes.DateType)
                ,col("headUrl"),col("height"),col("installDate").cast(DataTypes.DateType),col("installStatus"),col("isBarge")
                ,col("length"),col("licenceColor"),col("licenceNumber"),col("maxWeight"),col("midPlace")
                ,col("numberOfAxles"),col("operatorPlatform"),col("operatorPlatformId"),col("organizationId")
                ,col("organizationName"),col("outsideId"), functions.to_json(col("propertyMap")).as("propertyMap")
                ,col("protocolName"),col("protocolType"),col("runStatus"),col("seats"),col("serviceCode")
                ,col("simNumber"),col("startPlace"),col("startStation"),col("stationNumbers"),col("status")
                ,col("techLevel"),col("technologyLevelDate").cast(DataTypes.DateType),col("tenantId"),
                functions.to_json(col("terminals")).as("terminals"),col("totalMass"),col("tractionMass"),
                col("type"),col("uniqueLicence"),col("updateTime").cast(DataTypes.DateType),col("vehicleType"),
                col("wheelbase"),col("width"));
        dataset.show();
        dataset.select(functions.when(col("businessScope").eqNullSafe("省际定线旅游"), 1).when(col("businessScope").eqNullSafe("县内班车客运"), 3).otherwise(2)).show();
        dataset.write()
                .mode(SaveMode.Overwrite)
                .format("jdbc")
                .option("url",  "jdbc:mysql://10.50.125.141:3306/test_rule?useSSL=false&&characterEncoding=utf8")
                .option("dbTable", "dasd")
                .option("user", "root")
                .option("password", "hxc@069.root_mysql")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("truncate", true)
                .option("batchSize", 10000)
                .option("isolationLevel", "NONE")
                .save();
        dataset = dataset.select(col("B"));
        for (int i = 0; (i * 2) < 5; i++) {
            Dataset<Row> ds = dataset.limit(2);
            List<String> vals = ds.collectAsList().stream()
                    .map(row -> row.get(0).toString()).collect(Collectors.toList());
            System.err.println(Arrays.toString(vals.toArray()));
            dataset = dataset.except(ds);
            dataset.show();
        }
        dataset.foreach(row -> System.err.println(row.getString(1)));
        dataset.where(col("D").isNotNull().and(col("C").isNotNull())
                .and(col("A").isInCollection(Arrays.asList(1,2,3)))).show();
        List<org.apache.spark.sql.Column> ncols = Arrays.stream(dataset.columns()).map(functions::col).collect(Collectors.toList());
        ncols.add(functions.expr(String.format(dateFormat, "D", "yyyy-MM-dd", "yyyy/MM/dd")));
        ncols.add(lit(null).as("DD"));
        dataset.select(JavaConverters.asScalaIteratorConverter(ncols.iterator()).asScala().toSeq()).show();
        dataset.withColumn("DF", functions.expr(String.format(ConvertTypeEnum.ARAB.equals(type)
                ? c2aFormat : a2cFormat, "B", mode))).show();
        List<org.apache.spark.sql.Column> ncols = new ArrayList<>();
        ncols.add(col("A"));
        ncols.add(col("B"));
        ncols.add(functions.expr(String.format(repFormat, "B", "b", "@", 2)).as("BB"));
        ncols.add(functions.expr(String.format(repFormat, "C", "c", "A", 0)).as("C"));
        ncols.add(functions.expr(String.format(repFormat, "D", "01", "59", 1)).as("C"));
        ncols.add(functions.expr(String.format(fillFormat, "C", "000", 1, 10)).as("C"));
        ncols.add(functions.expr(String.format(fillFormat, "D", "2021-12-24", 0, 20)).as("D"));
        dataset.select(JavaConverters.asScalaIteratorConverter(ncols.iterator()).asScala().toSeq()).show();
        ncols.add(functions.expr(String.format(appendFormat, "B", "=", 1)));
        ncols.add(functions.expr(String.format(appendFormat, "C", ";", 1)));
        ncols.add(col("D"));
        dataset.withColumn("FF",
                functions.concat(JavaConverters.asScalaIteratorConverter(ncols.iterator()).asScala().toSeq())).show();
        dataset = dataset.withColumn("DD", functions.concat_ws("-", col("B"), col("C")))
                .withColumn("DD", functions.concat_ws(";", col("DD"), col("D")))
                .withColumn("A", functions.expr(String.format(appendFormat, "A", "123", 0)))
                .withColumn("hh", lit(null).cast(DataTypes.StringType));
       List<org.apache.spark.sql.Column> cols = new ArrayList<>();
          cols.add(col("A"));
          cols.add(col("B"));
          cols.add(col("C"));
          cols.add(col("D"));
          cols.add(col("FF").getItem(0).as("C1"));
          cols.add(col("FF").getItem(1).as("C2"));
          cols.add(col("FF").getItem(2).as("C3"));
          cols.add(col("FF").getItem(3).as("C4"));
          dataset = dataset.withColumn("FF", functions.split(col("C"), ","))
                  .select(JavaConverters.asScalaIteratorConverter(cols.iterator()).asScala().toSeq());
        sparkSession.close();

    }*/
}
