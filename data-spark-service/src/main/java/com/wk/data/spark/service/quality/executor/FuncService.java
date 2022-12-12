package com.wk.data.spark.service.quality.executor;

import com.vcolco.components.basis.result.SingleResult;
import com.wk.data.etl.facade.quality.dto.DataQualityDTO;
import com.wk.data.etl.facade.quality.dto.LogicCfgDTO;
import com.wk.data.etl.facade.quality.dto.NormalCfgDTO;
import com.wk.data.etl.facade.quality.vo.ReportInfoVO;
import com.wk.data.spark.infrastructure.util.quality.CaseOpera;
import com.wk.data.spark.infrastructure.util.quality.Opera;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.AccumulatorV2;

import java.util.List;

/**
 * @Author: smash_hq
 * @Date: 2021/12/30 10:43
 * @Description: 质量检测报告服务
 * @Version v1.0
 */

public interface FuncService {

    /**
     * 保存质量检测报告内容
     *
     * @param vo
     */
    void saveReport(ReportInfoVO vo);

    /**
     * 获取规则
     *
     * @param id
     * @return
     */
    SingleResult<DataQualityDTO> getRule(String id);

    /**
     * 读入数据
     *
     * @param dto
     * @return
     */
    Dataset<Row> reader(DataQualityDTO dto);

    /**
     * 检出表写出
     *
     * @param dataset
     * @param table
     */
    void writer(Dataset<Row> dataset, String table);

    /**
     * 常规检测
     *
     * @param data
     * @param normal
     * @return
     */
    Dataset<Row> normalCheck(Dataset<Row> data, List<NormalCfgDTO> normal);

    /**
     * 逻辑检测
     *
     * @param data
     * @param logic
     * @return
     */
    Dataset<Row> logicCheck(Dataset<Row> data, List<LogicCfgDTO> logic);

    /**
     * 获取报告
     *
     * @return
     */
    List<Opera> getMeta(AccumulatorV2<CaseOpera, List<CaseOpera>> acc);
}