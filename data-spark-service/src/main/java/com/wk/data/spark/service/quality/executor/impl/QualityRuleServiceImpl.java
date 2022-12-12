package com.wk.data.spark.service.quality.executor.impl;

import com.alibaba.fastjson.JSON;
import com.wk.data.etl.facade.quality.dto.DataQualityDTO;
import com.wk.data.etl.facade.quality.dto.LogicCfgDTO;
import com.wk.data.etl.facade.quality.dto.NormalCfgDTO;
import com.wk.data.etl.facade.quality.vo.InfoVO;
import com.wk.data.etl.facade.quality.vo.LogicVO;
import com.wk.data.etl.facade.quality.vo.NormalVO;
import com.wk.data.etl.facade.quality.vo.ReportInfoVO;
import com.wk.data.spark.infrastructure.util.RegisterUdf;
import com.wk.data.spark.infrastructure.util.quality.CaseAccumulatorV2;
import com.wk.data.spark.infrastructure.util.quality.CaseOpera;
import com.wk.data.spark.infrastructure.util.quality.Opera;
import com.wk.data.spark.service.quality.executor.FuncService;
import com.wk.data.spark.service.quality.executor.QualityRuleService;
import com.wk.data.spark.service.quality.executor.SparkRuleService;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.util.AccumulatorV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.wk.data.spark.infrastructure.util.General.OTHER_CHECK;

/**
 * @Author: smash_hq
 * @Date: 2021/8/26 10:14
 * @Description:
 * @Version v1.0
 */
@Component
public class QualityRuleServiceImpl implements QualityRuleService, Serializable, scala.Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(QualityRuleServiceImpl.class);
    public static final String RANK = "rowNumberRank";
    public static final String SYS_DEFAULT = "sys_default_tmp";

    @Autowired
    private SparkSession sparkSession;
    @Autowired
    private SparkRuleService sparkRuleService;
    @Autowired
    private FuncService func;

    private String scheduleId;
    private int execId;

    @Override
    public void qualityRules(String... args) throws AnalysisException {
        this.scheduleId = args[0];
        this.execId = Integer.parseInt(args[1]);
        LOGGER.warn("Spark 接收参数 id：{} ", scheduleId);
        LOGGER.warn("Spark 接收参数 execId：{} ", execId);
        // 1、获取基本信息
        DataQualityDTO base = func.getRule(scheduleId).getData();
        List<NormalCfgDTO> normal = base.getNormal();
        List<LogicCfgDTO> logic = base.getLogic();

        // 2、读数据（目前全部数据都在）
        Dataset<Row> data = func.reader(base).persist();
        Dataset<Row> checkData = data.withColumn(SYS_DEFAULT, functions.lit(1))
                .withColumn(RANK, functions.row_number().over(Window.partitionBy(SYS_DEFAULT).orderBy(SYS_DEFAULT)).cast(DataTypes.LongType))
                .drop(SYS_DEFAULT).cache();
        checkData.createTempView("tmp_view");
        RegisterUdf.udfRegister(sparkSession);

        // 3、先进行数据操作，再调用累加器
        AccumulatorV2<CaseOpera, List<CaseOpera>> acc = new CaseAccumulatorV2();
        sparkSession.sparkContext().register(acc, "accV2");
        Dataset<Row> normalCheck = func.normalCheck(checkData, normal);
        Dataset<Row> logicDataset = func.logicCheck(checkData, logic);
        normalCheck.union(logicDataset).collectAsList().forEach(row -> {
            String s = row.getAs(OTHER_CHECK);
            List<CaseOpera> list = JSON.parseArray(s, CaseOpera.class);
            if (!list.isEmpty()) {
                list.forEach(acc::add);
            }
        });
        List<Opera> operas = func.getMeta(acc);

        // 4、检测报告写入
        String detectionTable = base.getTableCode() + "_" + execId;
        List<LogicVO> logicReport = FuncServiceImpl.LOGIC_VOS;
        List<NormalVO> normalReport = FuncServiceImpl.NORMAL_VOS;
        List<CaseOpera> caseOperas = FuncServiceImpl.UNIQUE_LIST;
        caseOperas.stream().collect(Collectors.groupingBy(CaseOpera::getRowNumberRank)).forEach((k, v) -> {
            if (operas.stream().anyMatch(m -> m.getRowNumberRank().equals(k))) {
                operas.stream().filter(it -> it.getRowNumberRank().equals(k)).forEach(opera -> v.forEach(caseOpera -> {
                    if (opera.getLightAndSearch().containsKey(caseOpera.getCode())) {
                        opera.getLightAndSearch().get(caseOpera.getCode()).put(caseOpera.getType(), caseOpera.getJudge());
                    } else {
                        Map<String, Boolean> map = new HashMap<>();
                        map.put(caseOpera.getType(), caseOpera.getJudge());
                        opera.getLightAndSearch().put(caseOpera.getCode(), map);
                    }
                }));
            } else {
                Map<String, Map<String, Boolean>> map = new HashMap<>();
                v.forEach(caseOpera -> {
                    Map<String, Boolean> map1 = new HashMap<>();
                    map1.put(caseOpera.getType(), caseOpera.getJudge());
                    map.put(caseOpera.getCode(), map1);
                });
                operas.add(Opera.builder().rowNumberRank(k).lightAndSearch(map).build());
            }
        });

        Dataset<Opera> operaSet = sparkSession.createDataset(operas, Encoders.bean(Opera.class));
        Dataset<Row> check = operaSet.join(checkData, RANK).orderBy(RANK).drop(RANK);

        InfoVO info = InfoVO.builder().logic(logicReport).normal(normalReport).build();
        ReportInfoVO report = ReportInfoVO.builder().execId(execId).scheduleId(scheduleId)
                .layerCode(base.getLayerCode()).databaseCode(base.getDatabaseCode()).tableId(base.getTableId())
                .tableName(base.getTableName()).tableCode(base.getTableCode()).dataTotal(Math.toIntExact(data.count()))
                .filedTotal(data.schema().length()).normalTotal(base.getNormal().size()).logicTotal(base.getLogic().size())
                .detectionTable(detectionTable).detectionTotal(operas.size()).info(info)
                .build();
        func.saveReport(report);

        // 检出数据写出
        func.writer(check, detectionTable);
        sparkSession.catalog().dropTempView("tmp_view");
        sparkSession.close();
    }
}
