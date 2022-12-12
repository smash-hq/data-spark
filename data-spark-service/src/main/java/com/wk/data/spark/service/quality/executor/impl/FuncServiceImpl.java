package com.wk.data.spark.service.quality.executor.impl;

import com.alibaba.fastjson.JSON;
import com.vcolco.components.basis.result.SingleResult;
import com.wk.data.etl.facade.quality.api.QualityFacade;
import com.wk.data.etl.facade.quality.dto.*;
import com.wk.data.etl.facade.quality.vo.LogicVO;
import com.wk.data.etl.facade.quality.vo.NormalVO;
import com.wk.data.etl.facade.quality.vo.ReportInfoVO;
import com.wk.data.spark.infrastructure.util.quality.CaseOpera;
import com.wk.data.spark.infrastructure.util.quality.Opera;
import com.wk.data.spark.service.quality.executor.FuncService;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.util.AccumulatorV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.wk.data.spark.infrastructure.util.General.*;
import static com.wk.data.spark.service.quality.executor.impl.QualityRuleServiceImpl.RANK;

/**
 * @Author: smash_hq
 * @Date: 2021/12/30 10:44
 * @Description: 质量检测报告实现层
 * @Version v1.0
 */
@Component
public class FuncServiceImpl implements FuncService {
    private static final Logger LOGGER = LoggerFactory.getLogger(FuncServiceImpl.class);
    protected static final List<NormalVO> NORMAL_VOS = new ArrayList<>();
    protected static final List<LogicVO> LOGIC_VOS = new ArrayList<>();
    protected static final List<CaseOpera> UNIQUE_LIST = new ArrayList<>();

    @Autowired
    private QualityFacade facade;
    @Autowired
    private SparkSession sparkSession;

    @Value("${mongodb.data-lake}")
    private String lake;
    @Value("${mongodb.detection}")
    private String detection;
    @Value("${mysql.url}")
    private String url;
    @Value("${mysql.username}")
    private String user;
    @Value("${mysql.password}")
    private String password;
    @Value("${mysql.driver}")
    private String driver;

    @Override
    public void saveReport(ReportInfoVO vo) {
        facade.saveReport(vo);
    }

    @Override
    public SingleResult<DataQualityDTO> getRule(String id) {
        return facade.getSingle(id);
    }

    @Override
    public Dataset<Row> reader(DataQualityDTO dto) {
        String layerCode = dto.getLayerCode();
        String tableCode = dto.getTableCode();
        String db = dto.getDatabaseCode();
        if ("SDI".equals(layerCode)) {
            return mongoRead(tableCode);
        }
        return mysqlRead(db, tableCode);
    }

    @Override
    public void writer(Dataset<Row> dataset, String table) {
        dataset.write().mode(SaveMode.Overwrite).format("com.mongodb.spark.sql")
                .option("uri", detection)
                .option("database", "detection")
                .option("collection", table)
                .option("batchSize", 10000)
                .option("isolationLevel", "NONE")
                .save();
    }

    @Override
    public Dataset<Row> normalCheck(Dataset<Row> data, List<NormalCfgDTO> normal) {
        List<StructField> filed = new ArrayList<>();
        filed.add(DataTypes.createStructField(OTHER_CHECK, DataTypes.StringType, true));
        Dataset<Row> dataset = sparkSession.createDataFrame(new ArrayList<>(), DataTypes.createStructType(filed));
        for (int i = 0; i < normal.size(); i++) {
            NormalCfgDTO it = normal.get(i);
            NormalVO vo = NormalVO.builder().build();
            String code = it.getFiledCode();
            String json = JSON.toJSONString(it);
            int uniqueTotal = 0;
            AtomicInteger accuracyVarchar = new AtomicInteger();
            AtomicInteger accuracyValue = new AtomicInteger();
            AtomicInteger legality = new AtomicInteger();
            AtomicInteger range = new AtomicInteger();
            AtomicInteger integrality = new AtomicInteger();
            Dataset<Row> uCheck = null;
            if (Boolean.TRUE.equals(it.getUnique())) {
                uCheck = sparkSession.sql("SELECT * FROM (SELECT *,ROW_NUMBER() OVER(PARTITION BY " + code + " ORDER BY " + code + ") code_rank_num FROM tmp_view)t0 WHERE code_rank_num !=1")
                        .drop("code_rank_num").select(RANK).cache();
                List<Row> uRow = uCheck.collectAsList();
                uRow.forEach(row -> {
                    Long rank = row.getAs(RANK);
                    UNIQUE_LIST.add(CaseOpera.builder().code(code).rowNumberRank(rank).type(UNIQUE).judge(true).build());
                });
                uniqueTotal = Math.toIntExact(uCheck.count());
            }
            Dataset<Row> check = data.withColumn(OTHER_CHECK, functions.expr("normalCheck(" + code + "," + RANK + ",'" + code + "','" + json + "')"))
                    .select(OTHER_CHECK).where(functions.col(OTHER_CHECK).isNotNull()).cache();
            vo.setCode(code);
            vo.setUnique(uniqueTotal == 0 ? null : uniqueTotal);
            vo.setName(it.getFiledName());
            vo.setForm(it.getFiledType());
            vo.setRule(it.getRule());
            check.collectAsList().forEach(row -> {
                List<CaseOpera> caseOperas = JSON.parseArray(row.getAs(OTHER_CHECK), CaseOpera.class);
                caseOperas.forEach(op -> {
                    if (ACCURACY_VARCHAR.equals(op.getType())) {
                        vo.setAccuracyValue(accuracyVarchar.addAndGet(1));
                    }
                    if (RANGE.equals(op.getType())) {
                        vo.setRange(range.addAndGet(1));
                    }
                    if (BLANK.equals(op.getType())) {
                        vo.setBlank(range.addAndGet(1));
                    }
                    if (ACCURACY_VALUE.equals(op.getType())) {
                        vo.setAccuracyValue(accuracyValue.addAndGet(1));
                    }
                    if (LEGALITY.equals(op.getType())) {
                        vo.setLegality(legality.addAndGet(1));
                    }
                    if (INTEGRALITY.equals(op.getType())) {
                        vo.setIntegrality(integrality.addAndGet(1));
                    }
                });
            });
            dataset = dataset.union(check);
            NORMAL_VOS.add(vo);
            LOGGER.warn("常规检测字段：{}，唯一性：{}，其他：{}", code, (uCheck == null ? "无" : uCheck.count()), check.count());
        }
        return dataset;
    }

    @Override
    public Dataset<Row> logicCheck(Dataset<Row> data, List<LogicCfgDTO> logic) {
        List<StructField> filed = new ArrayList<>();
        filed.add(DataTypes.createStructField(OTHER_CHECK, DataTypes.StringType, true));
        Dataset<Row> dataset = sparkSession.createDataFrame(new ArrayList<>(), DataTypes.createStructType(filed));
        for (int i = 0; i < logic.size(); i++) {
            LogicCfgDTO it = logic.get(i);
            LogicVO logicVo = LogicVO.builder().build();
            Map<String, String> map = new HashMap<>();
            RuleIdDTO id = it.getIdRule();
            RuleTimeDTO time = it.getTimeRule();
            int times;
            if (id != null && StringUtils.isNotBlank(id.getHandle())) {
                map.put(id.getHandle(), id.getHandleCn());
                Dataset<Row> id18Dataset = data.withColumn(OTHER_CHECK, functions.expr("id18Logic(" + id.getHandle() + "," + RANK + ",'" + id.getHandle() + "')"))
                        .select(OTHER_CHECK).where(functions.col(OTHER_CHECK).isNotNull()).cache();
                times = Math.toIntExact(id18Dataset.count());
                dataset = dataset.union(id18Dataset);
                logicVo.setFileds(map);
                logicVo.setTime(times);
                logicVo.setRule(id.getName());
                logicVo.setRuleDetail(id.getDesc());
                LOGGER.warn("逻辑检测规则：{}，身份证校验：{}", map, times);
            } else if (time != null && StringUtils.isNotBlank(time.getHandle()) && StringUtils.isNotBlank(time.getBeCompare())) {
                map.put(time.getHandle(), time.getHandleCn());
                map.put(time.getBeCompare(), time.getBeCompareCn());
                Dataset<Row> timeDataset = data.withColumn(OTHER_CHECK, functions.expr("timeLogic(" + time.getHandle() + "," + time.getBeCompare() + "," + RANK + ",'" + time.getSymbol() + "','" + time.getHandle() + "','" + time.getBeCompare() + "')"))
                        .select(OTHER_CHECK).where(functions.col(OTHER_CHECK).isNotNull()).cache();
                times = Math.toIntExact(timeDataset.count());
                dataset = dataset.union(timeDataset);
                logicVo.setFileds(map);
                logicVo.setTime(times);
                logicVo.setRule(time.getName());
                logicVo.setRuleDetail(time.getDesc());
                LOGGER.warn("逻辑检测规则：{}，时间逻辑判断：{}", map, times);
            }
            LOGIC_VOS.add(logicVo);
        }
        return dataset;
    }

    @Override
    public List<Opera> getMeta(AccumulatorV2<CaseOpera, List<CaseOpera>> acc) {
        List<CaseOpera> list = new ArrayList<>();
        List<Opera> operas = new ArrayList<>();
        List<CaseOpera> collect = acc.value();
        Map<Long, List<CaseOpera>> map = collect.stream().collect(Collectors.groupingBy(CaseOpera::getRowNumberRank));
        map.forEach((k, v) -> {
            Map<String, Map<String, Boolean>> result = new HashMap<>();
            v.forEach(it -> {
                if (result.containsKey(it.getCode())) {
                    result.get(it.getCode()).put(it.getType(), it.getJudge());
                } else {
                    Map<String, Boolean> map1 = new HashMap<>();
                    map1.put(it.getType(), it.getJudge());
                    result.put(it.getCode(), map1);
                }
            });
            operas.add(Opera.builder().rowNumberRank(k).lightAndSearch(result).build());
        });
        return operas;
    }

    private Dataset<Row> mysqlRead(String db, String tableCode) {
        return sparkSession.read().format("jdbc")
                .option("url", String.format(url, db))
                .option("driver", driver)
                .option("user", user)
                .option("password", password)
                .option("dbtable", tableCode)
                .load();
    }

    private Dataset<Row> mongoRead(String tableCode) {
        return sparkSession.read().format("com.mongodb.spark.sql")
                .option("uri", lake)
                .option("collection", tableCode)
                .load();
    }
}
