package com.wk.data.spark.service.govern.executor.impl;

import com.alibaba.fastjson.JSON;
import com.vcolco.components.basis.result.SingleResult;
import com.wk.data.etl.facade.govern.api.GovernTaskFacade;
import com.wk.data.etl.facade.govern.dto.GovernTaskDetailDTO;
import com.wk.data.etl.facade.govern.dto.TaskNodeDTO;
import com.wk.data.etl.facade.govern.enums.TaskNodeEnum;
import com.wk.data.etl.facade.task.dto.CollectStatusDTO;
import com.wk.data.spark.infrastructure.util.RegisterUdf;
import com.wk.data.spark.infrastructure.util.constants.KafkaConsts;
import com.wk.data.spark.service.govern.executor.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @program: data-spark-job
 * @description: 数据治理逻辑
 * @author: gwl
 * @create: 2021-09-16 14:14
 **/
@Component
public class GovernServiceImpl implements GovernService, Serializable, scala.Serializable {

    private static Logger logger = LoggerFactory.getLogger(GovernServiceImpl.class);

    @Autowired
    private SparkSession sparkSession;
    @Autowired
    private GovernTaskFacade governTaskFacade;
    @Autowired
    private DataReaderService dataReaderService;
    @Autowired
    private DataWriterService dataWriterService;
    @Autowired
    private KafkaTemplate kafkaTemplate;


    @Override
    public void startCleanData(String[] args) throws Exception {
        if (args == null || args.length < 1 || StringUtils.isBlank(args[0])) {
            logger.error("数据清洗任务ID为空");
            throw new RuntimeException("数据清洗任务ID为空");
        }
        LocalDateTime dateTime = LocalDateTime.now();
//        StringBuilder sb = new StringBuilder();
//        sb.append("{\"id\":\"62bb0d3736a9d31985c5de25\",\"name\":\"驾培设备温度车辆集成\",\"type\":\"CLEAN\",\"projectId\":\"62a9502d65b53b7698ebd4f0\",\"projectName\":\"驾培数据项目\",\"desc\":\"驾培设备温度车辆集成\",\"nodes\":[{\"number\":\"nodeS3ZAnz8GCYfy7uaB\",\"name\":\"DATA_QUERY_nodeS3ZAnz8GCYfy7uaB\",\"type\":\"DATA_QUERY\",\"desc\":null,\"dependNumber\":[\"node19eFfrsQEb9LaG7K\"],\"data\":[{\"isOr\":false,\"code\":\"virtual_car\",\"operator\":\"eq\",\"value\":\"0\"}]},{\"number\":\"nodeJWMzyqnkwNytxwqH\",\"name\":\"DATA_QUERY_nodeJWMzyqnkwNytxwqH\",\"type\":\"DATA_QUERY\",\"desc\":null,\"dependNumber\":[\"nodeE976IFOHjI8PFUjq\"],\"data\":[{\"isOr\":false,\"code\":\"temperature\",\"operator\":\"gt\",\"value\":\"0\"}]},{\"number\":\"nodemdHVly6vaVENqG1N\",\"name\":\"DATA_INPUT_nodemdHVly6vaVENqG1N\",\"type\":\"DATA_INPUT\",\"desc\":null,\"dependNumber\":[],\"data\":{\"dataLayer\":\"SDI\",\"dbName\":\"四川驾培系统数据\",\"dbId\":\"62a94ff965b53b7698ebd4ef\",\"tableCode\":\"sdi_base_car2\",\"tableId\":\"62aac64b36a9d31985c5dd6e\",\"tableName\":\"教练车信息\",\"dbCode\":\"四川驾培系统数据\",\"cols\":[{\"isPrimaryKey\":null,\"isNotNull\":true,\"code\":\"id\",\"name\":\"教练车id\",\"type\":\"int\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":true,\"code\":\"school_code\",\"name\":\"驾校编号\",\"type\":\"int\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"unify_code\",\"name\":\"驾校统一编号\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"school_name\",\"name\":\"驾校名称\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"car_num\",\"name\":\"教练车编号(全国)\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"vin\",\"name\":\"车架号\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null,\"selectd\":true},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"engine_no\",\"name\":\"发动机号\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":true,\"code\":\"car_license\",\"name\":\"车牌号\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null,\"selectd\":true},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"plate_color\",\"name\":\"车牌颜色\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"plate_color_name\",\"name\":\"车辆颜色名称\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"car_photos\",\"name\":\"教练车图片url地址\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"model_id\",\"name\":\"车辆型号\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"car_type\",\"name\":\"培训车型\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"license_no\",\"name\":\"行驶证号\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"comments\",\"name\":\"备注\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"supervise_status\",\"name\":\"备案状态\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":true,\"code\":\"unavailable\",\"name\":\"是否不可用\",\"type\":\"int\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"gmt_buy\",\"name\":\"购买日期\",\"type\":\"date\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"dept_id\",\"name\":\"单位id\",\"type\":\"int\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"dept_name\",\"name\":\"所属部门名称\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":true,\"code\":\"gmt_create\",\"name\":\"新增时间\",\"type\":\"date\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":true,\"code\":\"gmt_modify\",\"name\":\"修改时间\",\"type\":\"date\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":true,\"code\":\"virtual_car\",\"name\":\"是否是虚拟车辆\",\"type\":\"int\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"is_old\",\"name\":\"是否老计时车辆\",\"type\":\"int\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"sc_id\",\"name\":\"四川老计时车辆id\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null}]}},{\"number\":\"nodeE976IFOHjI8PFUjq\",\"name\":\"DATA_INPUT_nodeE976IFOHjI8PFUjq\",\"type\":\"DATA_INPUT\",\"desc\":null,\"dependNumber\":[],\"data\":{\"dataLayer\":\"SDI\",\"dbName\":\"四川驾培系统数据\",\"dbId\":\"62a94ff965b53b7698ebd4ef\",\"tableCode\":\"sdi_equipment_temperature_6\",\"tableId\":\"62ba97b336a9d31985c5de15\",\"tableName\":\"驾培设备温度表-6月21日后\",\"dbCode\":\"四川驾培系统数据\",\"cols\":[{\"isPrimaryKey\":null,\"isNotNull\":true,\"code\":\"id\",\"name\":\"id\",\"type\":\"int\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":true,\"code\":\"car_license\",\"name\":\"车牌号\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"temperature\",\"name\":\"设备温度\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"trigger_time\",\"name\":\"触发时间\",\"type\":\"date\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"voltage_value\",\"name\":\"电压\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"whether_normal\",\"name\":\"whether_normal\",\"type\":\"int\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"collection_type\",\"name\":\"collection_type\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null}]}},{\"number\":\"node19eFfrsQEb9LaG7K\",\"name\":\"DATA_FILTER_node19eFfrsQEb9LaG7K\",\"type\":\"DATA_FILTER\",\"desc\":null,\"dependNumber\":[\"nodemdHVly6vaVENqG1N\"],\"data\":{\"code\":\"id\",\"type\":\"DATE\",\"mode\":\"LATAR\",\"cols\":[{\"isPrimaryKey\":null,\"isNotNull\":true,\"code\":\"id\",\"name\":\"教练车id\",\"type\":\"int\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":true,\"code\":\"school_code\",\"name\":\"驾校编号\",\"type\":\"int\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"unify_code\",\"name\":\"驾校统一编号\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"school_name\",\"name\":\"驾校名称\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"car_num\",\"name\":\"教练车编号(全国)\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"vin\",\"name\":\"车架号\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null,\"selectd\":true},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"engine_no\",\"name\":\"发动机号\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":true,\"code\":\"car_license\",\"name\":\"车牌号\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null,\"selectd\":true},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"plate_color\",\"name\":\"车牌颜色\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"plate_color_name\",\"name\":\"车辆颜色名称\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"car_photos\",\"name\":\"教练车图片url地址\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"model_id\",\"name\":\"车辆型号\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"car_type\",\"name\":\"培训车型\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"license_no\",\"name\":\"行驶证号\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"comments\",\"name\":\"备注\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"supervise_status\",\"name\":\"备案状态\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":true,\"code\":\"unavailable\",\"name\":\"是否不可用\",\"type\":\"int\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"gmt_buy\",\"name\":\"购买日期\",\"type\":\"date\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"dept_id\",\"name\":\"单位id\",\"type\":\"int\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"dept_name\",\"name\":\"所属部门名称\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":true,\"code\":\"gmt_create\",\"name\":\"新增时间\",\"type\":\"date\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":true,\"code\":\"gmt_modify\",\"name\":\"修改时间\",\"type\":\"date\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":true,\"code\":\"virtual_car\",\"name\":\"是否是虚拟车辆\",\"type\":\"int\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"is_old\",\"name\":\"是否老计时车辆\",\"type\":\"int\",\"precision\":\"--\",\"standardId\":null},{\"isPrimaryKey\":null,\"isNotNull\":false,\"code\":\"sc_id\",\"name\":\"四川老计时车辆id\",\"type\":\"string\",\"precision\":\"--\",\"standardId\":null}]}},{\"number\":\"nodezxCKNgDgZXm4JQeZ\",\"name\":\"DATA_INTEGRATE_nodezxCKNgDgZXm4JQeZ\",\"type\":\"DATA_INTEGRATE\",\"desc\":null,\"dependNumber\":[\"nodeJWMzyqnkwNytxwqH\",\"nodeS3ZAnz8GCYfy7uaB\"],\"data\":{\"joins\":[{\"code\":\"sdi_base_car2\",\"name\":\"教练车信息\",\"nickname\":\"T1\",\"leftField\":\"car_license\",\"rightField\":\"car_license\",\"preNumber\":\"nodeS3ZAnz8GCYfy7uaB\"}],\"code\":\"sdi_equipment_temperature_6\",\"name\":\"驾培设备温度表-6月21日后\",\"nickname\":\"T0\",\"preNumber\":\"nodeJWMzyqnkwNytxwqH\",\"fields\":[{\"code\":\"T0.id\",\"name\":\"id\",\"type\":\"int\",\"alias\":\"T0\"},{\"code\":\"T0.car_license\",\"name\":\"车牌号\",\"type\":\"string\",\"alias\":\"T0\"},{\"code\":\"T0.temperature\",\"name\":\"设备温度\",\"type\":\"string\",\"alias\":\"T0\"},{\"code\":\"T0.trigger_time\",\"name\":\"触发时间\",\"type\":\"date\",\"alias\":\"T0\"},{\"code\":\"T0.voltage_value\",\"name\":\"电压\",\"type\":\"string\",\"alias\":\"T0\"},{\"code\":\"T0.whether_normal\",\"name\":\"whether_normal\",\"type\":\"int\",\"alias\":\"T0\"},{\"code\":\"T0.collection_type\",\"name\":\"collection_type\",\"type\":\"string\",\"alias\":\"T0\"},{\"code\":\"T1.id\",\"name\":\"教练车id\",\"type\":\"int\",\"alias\":\"T1\"},{\"code\":\"T1.school_code\",\"name\":\"驾校编号\",\"type\":\"int\",\"alias\":\"T1\"},{\"code\":\"T1.unify_code\",\"name\":\"驾校统一编号\",\"type\":\"string\",\"alias\":\"T1\"},{\"code\":\"T1.school_name\",\"name\":\"驾校名称\",\"type\":\"string\",\"alias\":\"T1\"},{\"code\":\"T1.car_num\",\"name\":\"教练车编号(全国)\",\"type\":\"string\",\"alias\":\"T1\"},{\"code\":\"T1.vin\",\"name\":\"车架号\",\"type\":\"string\",\"alias\":\"T1\"},{\"code\":\"T1.engine_no\",\"name\":\"发动机号\",\"type\":\"string\",\"alias\":\"T1\"},{\"code\":\"T1.car_license\",\"name\":\"车牌号\",\"type\":\"string\",\"alias\":\"T1\"},{\"code\":\"T1.plate_color\",\"name\":\"车牌颜色\",\"type\":\"string\",\"alias\":\"T1\"},{\"code\":\"T1.plate_color_name\",\"name\":\"车辆颜色名称\",\"type\":\"string\",\"alias\":\"T1\"},{\"code\":\"T1.car_photos\",\"name\":\"教练车图片url地址\",\"type\":\"string\",\"alias\":\"T1\"},{\"code\":\"T1.model_id\",\"name\":\"车辆型号\",\"type\":\"string\",\"alias\":\"T1\"},{\"code\":\"T1.car_type\",\"name\":\"培训车型\",\"type\":\"string\",\"alias\":\"T1\"},{\"code\":\"T1.license_no\",\"name\":\"行驶证号\",\"type\":\"string\",\"alias\":\"T1\"},{\"code\":\"T1.comments\",\"name\":\"备注\",\"type\":\"string\",\"alias\":\"T1\"},{\"code\":\"T1.supervise_status\",\"name\":\"备案状态\",\"type\":\"string\",\"alias\":\"T1\"},{\"code\":\"T1.unavailable\",\"name\":\"是否不可用\",\"type\":\"int\",\"alias\":\"T1\"},{\"code\":\"T1.gmt_buy\",\"name\":\"购买日期\",\"type\":\"date\",\"alias\":\"T1\"},{\"code\":\"T1.dept_id\",\"name\":\"单位id\",\"type\":\"int\",\"alias\":\"T1\"},{\"code\":\"T1.dept_name\",\"name\":\"所属部门名称\",\"type\":\"string\",\"alias\":\"T1\"},{\"code\":\"T1.gmt_create\",\"name\":\"新增时间\",\"type\":\"date\",\"alias\":\"T1\"},{\"code\":\"T1.gmt_modify\",\"name\":\"修改时间\",\"type\":\"date\",\"alias\":\"T1\"},{\"code\":\"T1.virtual_car\",\"name\":\"是否是虚拟车辆\",\"type\":\"int\",\"alias\":\"T1\"},{\"code\":\"T1.is_old\",\"name\":\"是否老计时车辆\",\"type\":\"int\",\"alias\":\"T1\"},{\"code\":\"T1.sc_id\",\"name\":\"四川老计时车辆id\",\"type\":\"string\",\"alias\":\"T1\"}]}},{\"number\":\"nodeKS0jOi9sXCG4izX4\",\"name\":\"INTEGRATE_OUTPUT_nodeKS0jOi9sXCG4izX4\",\"type\":\"INTEGRATE_OUTPUT\",\"desc\":null,\"dependNumber\":[\"nodezxCKNgDgZXm4JQeZ\"],\"data\":{\"cols\":[{\"name\":\"所属省\",\"code\":\"province\",\"type\":\"varchar\",\"first\":{\"nickname\":\"\"},\"second\":{\"nickname\":\"\"},\"third\":{\"nickname\":\"\"},\"isPrimaryKey\":false,\"length\":\"\"},{\"name\":\"所属市\",\"code\":\"city\",\"type\":\"varchar\",\"first\":{\"nickname\":\"\"},\"second\":{\"nickname\":\"\"},\"third\":{\"nickname\":\"\"},\"isPrimaryKey\":false,\"length\":\"\"},{\"name\":\"所属区县\",\"code\":\"county\",\"type\":\"varchar\",\"first\":{\"nickname\":\"\"},\"second\":{\"nickname\":\"\"},\"third\":{\"nickname\":\"\"},\"isPrimaryKey\":false,\"length\":\"\"},{\"name\":\"所属驾校名称\",\"code\":\"school_name\",\"type\":\"varchar\",\"first\":{\"table\":\"sdi_base_car2\",\"field\":\"school_name\",\"nickname\":\"T1\"},\"second\":{\"nickname\":\"\"},\"third\":{\"nickname\":\"\"},\"isPrimaryKey\":false,\"length\":\"\"},{\"name\":\"所属驾校代码\",\"code\":\"school_code\",\"type\":\"integer\",\"first\":{\"table\":\"sdi_base_car2\",\"field\":\"school_code\",\"nickname\":\"T1\"},\"second\":{\"nickname\":\"\"},\"third\":{\"nickname\":\"\"},\"isPrimaryKey\":false,\"length\":\"\"},{\"name\":\"车牌号\",\"code\":\"PlateNumber\",\"type\":\"varchar\",\"first\":{\"table\":\"sdi_base_car2\",\"field\":\"car_license\",\"nickname\":\"T1\"},\"second\":{\"nickname\":\"\"},\"third\":{\"nickname\":\"\"},\"isPrimaryKey\":false,\"length\":\"\"},{\"name\":\"车牌颜色名称\",\"code\":\"PlateColor\",\"type\":\"varchar\",\"first\":{\"table\":\"sdi_base_car2\",\"field\":\"plate_color_name\",\"nickname\":\"T1\"},\"second\":{\"nickname\":\"\"},\"third\":{\"nickname\":\"\"},\"isPrimaryKey\":false,\"length\":\"\"},{\"name\":\"车架号\",\"code\":\"VINCode\",\"type\":\"varchar\",\"first\":{\"table\":\"sdi_base_car2\",\"field\":\"vin\",\"nickname\":\"T1\"},\"second\":{\"nickname\":\"\"},\"third\":{\"nickname\":\"\"},\"isPrimaryKey\":false,\"length\":\"\"},{\"name\":\"所属设备编号\",\"code\":\"equipment_id\",\"type\":\"integer\",\"first\":{\"nickname\":\"\"},\"second\":{\"nickname\":\"\"},\"third\":{\"nickname\":\"\"},\"isPrimaryKey\":false,\"length\":\"\"},{\"name\":\"设备状态\",\"code\":\"status\",\"type\":\"varchar\",\"first\":{\"nickname\":\"\"},\"second\":{\"nickname\":\"\"},\"third\":{\"nickname\":\"\"},\"isPrimaryKey\":false,\"length\":\"\"},{\"name\":\"设备厂商\",\"code\":\"manufacturer\",\"type\":\"varchar\",\"first\":{\"nickname\":\"\"},\"second\":{\"nickname\":\"\"},\"third\":{\"nickname\":\"\"},\"isPrimaryKey\":false,\"length\":\"\"},{\"name\":\"设备软件版本\",\"code\":\"upgrade_version\",\"type\":\"varchar\",\"first\":{\"nickname\":\"\"},\"second\":{\"nickname\":\"\"},\"third\":{\"nickname\":\"\"},\"isPrimaryKey\":false,\"length\":\"\"},{\"name\":\"设备温度\",\"code\":\"temperature\",\"type\":\"varchar\",\"first\":{\"table\":\"sdi_equipment_temperature_6\",\"field\":\"temperature\",\"nickname\":\"T0\"},\"second\":{\"nickname\":\"\"},\"third\":{\"nickname\":\"\"},\"isPrimaryKey\":false,\"length\":\"\"},{\"name\":\"温度产生时间\",\"code\":\"trigger_time\",\"type\":\"timestamp\",\"first\":{\"table\":\"sdi_equipment_temperature_6\",\"field\":\"trigger_time\",\"nickname\":\"T0\"},\"second\":{\"nickname\":\"\"},\"third\":{\"nickname\":\"\"},\"isPrimaryKey\":false,\"length\":\"\"},{\"name\":\"车辆是否不可用\",\"code\":\"unavailable\",\"type\":\"integer\",\"first\":{\"nickname\":\"\"},\"second\":{\"nickname\":\"\"},\"third\":{\"nickname\":\"\"},\"isPrimaryKey\":false,\"length\":\"\"}],\"type\":1,\"dataLayer\":\"DWI\",\"dbName\":\"驾培数据\",\"dbId\":\"62afdca436a9d31985c5dd8b\",\"tableCode\":\"dwi_car_temperature\",\"tableId\":\"62bb0b3936a9d31985c5de24\",\"tableName\":\"温度车辆集成\",\"dbCode\":\"jpsj\",\"scope\":0}}]}");
//        GovernTaskDetailDTO task = JSON.parseObject(sb.toString(), GovernTaskDetailDTO.class);
        String taskId = args[0];
        String uuid = UUID.randomUUID().toString();
        try {
            GovernTaskDetailDTO task = getGovernTaskInfo(taskId);
            List<TaskNodeDTO> nodes = task.getNodes();
            RegisterUdf.udfRegister(sparkSession);
            // 1、获取到所有的数据输入节点
            List<TaskNodeDTO> inputs = nodes.stream().filter(it -> TaskNodeEnum.DATA_INPUT.getEnName().equals(it.getType())).collect(Collectors.toList());
            // 2、创建加载数据的service，把数据装载近readers中
            List<String> pnms = dataReaderService.loadDataSet(inputs);
            // 3、剩余未执行的非数据输入和数据输出的节点的ID
            List<String> govnms = nodes.stream().filter(it ->
                    !TaskNodeEnum.DATA_INPUT.getEnName().equals(it.getType()) &&
                    !TaskNodeEnum.DATA_OUTPUT.getEnName().equals(it.getType()) &&
                    !TaskNodeEnum.INTEGRATE_OUTPUT.getEnName().equals(it.getType()))
                    .map(TaskNodeDTO::getNumber).collect(Collectors.toList());
            // 已执行完成 的任务节点编号集合
            List<String> completedNos = new ArrayList<>(pnms);
            do {
                // 4、根据前一执行节点的编号找出依赖它的所有任务节点，并且是非输入和输出的节点，返回对应节点编号集
                pnms = getDependNodes(nodes, pnms, govnms, completedNos);
                // 5、把已执行的节点编号从未执行的编号中去除掉
                govnms.remove(pnms);
            } while (govnms != null && !govnms.isEmpty() && pnms != null && !pnms.isEmpty());
            // 6、寻找依赖了output的节点，
            List<TaskNodeDTO> outputs = nodes.stream().filter(it ->
                    TaskNodeEnum.DATA_OUTPUT.getEnName().equals(it.getType()) ||
                    TaskNodeEnum.INTEGRATE_OUTPUT.getEnName().equals(it.getType())
            ).collect(Collectors.toList());
            // 7、创建加载数据的service，把数据装载近readers中
            dataWriterService.writeData(outputs);
        } catch (Exception e) {
            logger.error("数据清洗任务执行失败", e);
            sendCollectMessage(taskId, dateTime, uuid, false);
            throw e;
        } finally {
            sparkSession.close();
        }
        sendCollectMessage(taskId, dateTime, uuid, true);
    }

    /**
     * 推送执行的结果消息到kafka
     */
    private void sendCollectMessage(String id, LocalDateTime dateTime, String uuid, boolean status) {
        CollectStatusDTO collectStatus = CollectStatusDTO.builder().id(id)
                .day(dateTime.toLocalDate().toString())
                .gmtStart(Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant()))
                .status(status).build();
        kafkaTemplate.send(KafkaConsts.GOVERB_TASK_STATUS_TOPIC, uuid, JSON.toJSONString(collectStatus));
    }

    private List<String> getDependNodes(List<TaskNodeDTO> nodes, List<String> pnms, List<String> govnms,
                                        List<String> completedNos)
            throws Exception {
        List<String> dnms = new ArrayList<>();
        for (TaskNodeDTO node : nodes) {
            TaskNodeEnum type = TaskNodeEnum.getEnumByEnName(node.getType());
            // 检测该节点是否为下一个该执行的任务节点
            boolean flag = checkIsNextNode(node, pnms, govnms, completedNos);
            if (!flag) {
                continue;
            }
            Transform transform = null;
            switch (type) {
                case DATA_CLEAN:
                    transform = new DataCleanService(node, sparkSession);
                    break;
                case DATA_FILTER:
                    transform = new DataFilterService(node, sparkSession);
                    break;
                case DATA_DESENSITISE:
                    transform = new DataEncryptService(node, sparkSession);
                    break;
                case DATA_CONVERT:
                    transform = new DataConvertService(node, sparkSession);
                    break;
                case DATA_SPLIT:
                    transform = new DataSplitService(node, sparkSession);
                    break;
                case DATA_UNION:
                    transform = new DataUnionService(node, sparkSession);
                    break;
                case DATA_EXPLODE:
                    transform = new DataExplodeService(node, sparkSession);
                    break;
                case DATA_QUERY:
                    transform = new DataQueryFilterService(node, sparkSession);
                    break;
                case DATA_INTEGRATE:
                    transform = new DataJoinService(node, sparkSession);
                    break;
                case BASE_CONVERT:
                    transform = new HexConvService(node, sparkSession);
                    break;
                case DISLOCATION_OPERATION:
                    transform = new DataLagService(node, sparkSession);
                    break;
                case SQL_OPERATION:
                    transform = new DataSQLService(node, sparkSession);
                    break;
                case INDEX_STATISTICS:
                    transform = new DataGroupingService(node, sparkSession);
                    break;
                case CONDITION_CONVERT:
                    transform = new DataConvertByConditionService(node, sparkSession);
                    break;
                case DATA_OPERATION:
                    transform = new DataFigureService(node, sparkSession);
                    break;
                default:
                    logger.error("暂不支持的数据处理规则：{}", JSON.toJSONString(node));
                    break;
            }
            if (transform != null) {
                transform.execute();
                dnms.add(node.getNumber());
                completedNos.add(node.getNumber());
            }

        }
        return dnms;
    }

    private boolean checkIsNextNode(TaskNodeDTO node, List<String> pnms, List<String> govnms,
                                    List<String> completedNos) {
        List<String> dns = node.getDependNumber();
        if (!govnms.contains(node.getNumber()) || dns == null || dns.isEmpty()) {
            return false;
        }
        // 过滤掉依赖节点中的错误编号，即去除输出节点外 所有节点中不包含的节点编号
        List<String> dnos = dns.stream().filter(it -> StringUtils.isNotBlank(it) && (govnms.contains(it)
                || completedNos.contains(it))).collect(Collectors.toList());
        // TODO 若节点依赖的任务节点已全部执行完成，并且依赖节点是上次执行完成的节点，则认为是下次执行节点
        return !dnos.isEmpty() && completedNos.containsAll(dnos) && pnms.stream().anyMatch(it -> dnos.contains(it));
    }

    private GovernTaskDetailDTO getGovernTaskInfo(String taskId) {
        SingleResult<GovernTaskDetailDTO> res = governTaskFacade.getGovernTaskById(taskId);
        if (res == null || !res.isSuccess() || res.getData() == null) {
            logger.error("facade查询治理任务【{}失败：{}", taskId, res);
            throw new RuntimeException("facade查询治理任务失败");
        }
        GovernTaskDetailDTO task = res.getData();
        List<TaskNodeDTO> nodes = task.getNodes();
        if (nodes != null) {
            nodes = nodes.stream().filter(it -> it != null && StringUtils.isNotBlank(it.getType()) &&
                    TaskNodeEnum.getEnumByEnName(it.getType()) != null).collect(Collectors.toList());
        }
        if (nodes == null || nodes.isEmpty() || nodes.size() < 2) {
            logger.error("数据治理任务的处理节点配置不完整:{}", JSON.toJSONString(task));
            throw new RuntimeException("治理任务的数据处理操作流程配置不完整");
        }
        if (nodes.stream().noneMatch(it -> TaskNodeEnum.DATA_INPUT.getEnName().equals(it.getType()))) {
            logger.error("数据治理任务缺少数据输入节点:{}", JSON.toJSONString(task));
            throw new RuntimeException("治理任务的数据处理流程缺少 -数据输入- 节点");
        }
        if (nodes.stream().noneMatch(it -> TaskNodeEnum.DATA_OUTPUT.getEnName().equals(it.getType()) ||
                TaskNodeEnum.INTEGRATE_OUTPUT.getEnName().equals(it.getType()))) {
            logger.error("数据治理任务缺少数据输出节点:{}", JSON.toJSONString(task));
            throw new RuntimeException("治理任务的数据处理流程缺少 -数据输出- 节点");
        }
        task.setTaskJson(null);
        task.setNodes(nodes);
        return task;
    }

    @Override
    public void startSplitData(String[] args) throws Exception {
        startCleanData(args);
      /*  RegisterUdf.udfRegister(sparkSession);
        throw new RuntimeException("开发中");*/
        /*if (args == null || args.length < 1 || StringUtils.isNotBlank(args[0])) {
            logger.error("数据拆分任务ID为空");
        }
        GovernTaskDetailDTO task = getGovernTaskInfo(args[0]);
        List<TaskNodeDTO> nodes = task.getNodes();*/

    }

    @Override
    public void startConvergeData(String[] args) throws Exception {
        startCleanData(args);
      /*  RegisterUdf.udfRegister(sparkSession);
        throw new RuntimeException("开发中");*/
        /*if (args == null || args.length < 1 || StringUtils.isNotBlank(args[0])) {
            logger.error("数据聚合任务ID为空");
        }
        GovernTaskDetailDTO task = getGovernTaskInfo(args[0]);
        List<TaskNodeDTO> nodes = task.getNodes();*/
    }

}
