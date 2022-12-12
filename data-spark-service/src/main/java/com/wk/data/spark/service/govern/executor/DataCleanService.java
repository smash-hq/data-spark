package com.wk.data.spark.service.govern.executor;

import com.alibaba.fastjson.JSON;
import com.wk.data.etl.facade.govern.dto.TaskNodeDTO;
import com.wk.data.etl.facade.govern.dto.clean.*;
import com.wk.data.etl.facade.govern.enums.AssignValueEnum;
import com.wk.data.etl.facade.govern.enums.ConvertModeEnum;
import com.wk.data.etl.facade.govern.enums.ConvertTypeEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static scala.Predef.wrapRefArray;

/**
 * @program: data-spark-job
 * @description: 数据清洗
 * @author: gwl
 * @create: 2021-12-10 15:33
 **/
public class DataCleanService extends Transform implements Serializable, scala.Serializable {

    private static Logger logger = LoggerFactory.getLogger(DataCleanService.class);

    private TaskNodeDTO node;

    private SparkSession session;


    public DataCleanService(TaskNodeDTO node, SparkSession sparkSession) {
        this.node = node;
        this.session = sparkSession;
    }

    private static String appendFormat = "filedAppend(cast(%s as string), '%s', %d)";
    private static String fillFormat = "filedFill(cast(%s as string), '%s', %d, %d)";
    private static String repFormat = "filedReplace(cast(%s as string), '%s', '%s', %d)";
    private static String fappFormat = "filedAppendChar(cast(%s as string), '%s', '%s', %d)";
    private static String rmpFormat = "filedDeleteByChar(cast(%s as string), '%s', %d, %d)";
    private static String rmaFormat = "filedDeleteAmount(cast(%s as string), %d, %d)";
    private static String rmFormat = "filedDeleteChar(cast(%s as string), '%s', %d)";
    private static String dbFormat = "deleteBlank(cast(%s as string), %d)";
    private static String crFormat = "filedCarry(cast(%s as string), %s, %d)";
    private static String s2nFormat = "some2Null(cast(%s as string), '%s')";
    private static String n2sFormat = "null2Some(%s, '%s')";
    private static String e2sFormat = "empty2Some(%s, '%s')";
    private static String a2cFormat = "arabicToCn(cast(%s as string), %d)";
    private static String c2aFormat = "cnToArabic(cast(%s as string), %d)";
    private static String dtFormat = "datetimeFormat(cast(%s as string), '%s', '%s')";
    private static String dateFormat = "dateFormat(cast(%s as string), '%s', '%s')";
    private static String timeFormat = "timeFormat(cast(%s as string), '%s', '%s')";
    private static String subStringLR = "subStringLR(cast(%s as string), %d, %d, %d)";
    private static String assginValue = "assignValue(cast(%s as string), cast(%s as string), %d)";

    @Override
    public void execute() throws Exception {
        List<String> dns = node.getDependNumber();
        String upTempView = dns.stream().filter(StringUtils::isNotBlank).findFirst().get();
        String tempView = node.getNumber();
        List<CleanRuleDTO> rules = JSON.parseArray(JSON.toJSONString(node.getData()), CleanRuleDTO.class);
        cleanData(rules, upTempView, tempView);
    }

    private void cleanData(List<CleanRuleDTO> rules, String upTempView, String tempView) throws Exception {
        Dataset<Row> dataset = session.table(upTempView);
        if (dataset == null || dataset.count() <= 0) {
            logger.warn("数据清洗节点的源数据记录为空：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            return;
        }
        if (rules == null || rules.isEmpty()) {
            logger.warn("数据清洗节点配置的清洗规则为空：{}", JSON.toJSONString(node));
            dataset.createOrReplaceTempView(tempView);
            session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
            return;
        }
        for (CleanRuleDTO rule : rules) {
            String code = rule.getCode();
            switch (code) {
                case "SPLIT": // 按分隔符拆分列
                    dataset = colsSplit(rule, dataset);
                    break;
                case "MERGE": // 多个字段合并
                    dataset = colsMerge(rule, dataset);
                    break;
                case "STR_FILL": // 字符串填充
                    dataset = colsStrFill(rule, dataset);
                    break;
                case "STR_REPLACE": // 替换字符串
                    dataset = colsStrReplace(rule, dataset);
                    break;
                case "APPED_STR_INDEX": // 在指定位置添加字符（串）
                    dataset = colsAppendStrIndex(rule, dataset);
                    break;
                case "APPED_STR": // 在字符串前后添加字符（串）
                    dataset = colsAppendStr(rule, dataset);
                    break;
                case "REMOVE_STR_INDEX": // 删除字符串前后字符
                    dataset = colsRemoveStrIndex(rule, dataset);
                    break;
                case "REMOVE_STR_COUNT": // 删除指定数目的字符
                    dataset = colsRemoveStrCount(rule, dataset);
                    break;
                case "REMOVE_STR": // 删除字符（串）
                    dataset = colsRemoveStr(rule, dataset);
                    break;
                case "REMOVE_SPACE": // 删除空格
                    dataset = colsRemoveBlank(rule, dataset);
                    break;
                case "TRUNCATION": // 舍位
                    dataset = colsTruncation(rule, dataset);
                    break;
                case "REPLACE_NULL": // 指定值替换为NULL
                    dataset = colsReplaceNull(rule, dataset);
                    break;
                case "NULL_REPLACE": // NULL替换为指定值
                    dataset = colsNullReplace(rule, dataset);
                    break;
                case "BLANK_REPLACE": // 空值替换为指定值
                    dataset = colsBlankReplace(rule, dataset);
                    break;
                case "IDCARD_CONVERT": // 身份证位数转换
                    dataset = colsIDCardConvert(rule, dataset);
                    break;
                case "ARAB_CHINESE": // 阿拉伯中文数字转换
                    dataset = colsArab2Chinese(rule, dataset);
                    break;
                case "DATETIME_FORMAT": // 日期时间字符串格式转换
                    dataset = colsDatetimeFormat(rule, dataset, dtFormat);
                    break;
                case "DATE_FORMAT": // 日期字符串格式转换
                    dataset = colsDatetimeFormat(rule, dataset, dateFormat);
                    break;
                case "TIME_FORMAT": // 时间字符串格式转换
                    dataset = colsDatetimeFormat(rule, dataset, timeFormat);
                    break;
                case "SUBSTRING": // 时间字符串格式转换
                    dataset = colsSubString(rule, dataset);
                    break;
                case "ASSIGN_VALUE": // 时间字符串格式转换
                    dataset = colsAssignValue(rule, dataset);
                    break;
                default:
                    logger.warn("暂不支持的清洗规则：{}", JSON.toJSON(rule));
                    break;
            }
        }
        dataset.createOrReplaceTempView(tempView);
        session.catalog().cacheTable(tempView, StorageLevel.MEMORY_AND_DISK());
    }

    // todo 字符串截取功能
    private Dataset<Row> colsSubString(CleanRuleDTO rule, Dataset<Row> dataset) {
        String text = JSON.toJSONString(rule.getData());
        SubStringDTO dto = JSON.parseObject(text, SubStringDTO.class);
        if (dto == null || dto.getCode() == null || dto.getNewCode() == null || dto.getMode() == null) {
            logger.warn("数据清洗，字符串截取的规则配置不完整：{}", rule);
            return dataset;
        }
        String code = dto.getCode();
        String newCode = dto.getNewCode();
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        if (!cols.contains(code)) {
            logger.warn("数据清洗 字符串截取{}在数据源列集中不存在：{}", code, text);
            return dataset;
        }
        Integer mode = dto.getMode();
        Integer start = dto.getStart();
        Integer end = dto.getEnd();
        List<org.apache.spark.sql.Column> ncols = cols.stream().map(functions::col)
                .collect(Collectors.toList());
        dataset = dataset.withColumn(newCode, functions.expr(String.format(subStringLR, code, mode, start, end)).as(newCode));
        return dataset;
    }

    // todo 特殊赋值功能
    private Dataset<Row> colsAssignValue(CleanRuleDTO rule, Dataset<Row> dataset) {
        String text = JSON.toJSONString(rule.getData());
        AssignValueDTO dto = JSON.parseObject(text, AssignValueDTO.class);
        if (dto == null || dto.getCode() == null || dto.getSourceCode() == null) {
            logger.warn("数据清洗，字段赋值的规则配置不完整：{}", rule);
            return dataset;
        }
        AssignValueEnum assign = dto.getAssign();
        String code = dto.getCode();
        String tocode = dto.getSourceCode();
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        if (!cols.contains(code)) {
            logger.warn("数据清洗 字段赋值的被赋值字段{}在数据源列集中不存在：{}", code, text);
            return dataset;
        }
        if (!cols.contains(tocode)) {
            logger.warn("数据清洗 字段赋值的赋值字段{}在数据源列集中不存在：{}", tocode, text);
            return dataset;
        }
        int i = 0;
        switch (assign) {   // 使用自定义函数时对于枚举类型传递支撑性不佳，故进行枚举值转置
            case ANYWAY:
                i = 1;
                break;
            case ISBLANK:
                i = 2;
                break;
            case ISNULL:
                i = 3;
                break;
            default:
                break;
        }
        int finalI = i;
        List<org.apache.spark.sql.Column> ncols = cols.stream().map(it -> !code.equals(it) ? col(it) :
                        functions.expr(String.format(assginValue, code, tocode, finalI)).as(it))
                .collect(Collectors.toList());
        dataset = dataset.select((Seq) wrapRefArray(ncols.toArray()));
        return dataset;
    }

    private Dataset<Row> colsRemoveBlank(CleanRuleDTO rule, Dataset<Row> dataset) {
        String rstr = JSON.toJSONString(rule.getData());
        StrTrimDTO dto = JSON.parseObject(rstr, StrTrimDTO.class);
        if (dto == null || dto.getCodes() == null || dto.getCodes().isEmpty()) {
            logger.warn("数据清洗 删除空格的规则配置不完整：{}", rstr);
            return dataset;
        }
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<String> list = dto.getCodes().stream().filter(it -> StringUtils.isNotBlank(it) && cols.contains(it))
                .collect(Collectors.toList());
        if (list.isEmpty()) {
            logger.warn("数据清洗 删除空格规则配置字段跟实际数据字段不符：{}", rstr);
            return dataset;
        }
        List<org.apache.spark.sql.Column> ncols = cols.stream().map(it -> !list.contains(it) ? col(it) :
                        functions.expr(String.format(dbFormat, it, dto.getPos())).as(it))
                .collect(Collectors.toList());
        dataset = dataset.select((Seq) wrapRefArray(ncols.toArray()));
        return dataset;
    }

    private Dataset<Row> colsDatetimeFormat(CleanRuleDTO rule, Dataset<Row> dataset, String format) {
        String rstr = JSON.toJSONString(rule.getData());
        StrDateFormatDTO dto = JSON.parseObject(rstr, StrDateFormatDTO.class);
        if (dto == null || StringUtils.isBlank(dto.getCode()) || StringUtils.isBlank(dto.getSourcePattern())
                || StringUtils.isBlank(dto.getTargetPattern())) {
            logger.warn("数据清洗 日期时间转换的规则配置为空：{}", rstr);
            return dataset;
        }
        String code = dto.getCode();
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        if (!cols.contains(code)) {
            logger.warn("数据清洗 日期时间换规则配置的字段{}在数据源列集中不存在：{}", code, rstr);
            return dataset;
        }
        List<org.apache.spark.sql.Column> ncols = cols.stream().filter(it -> !cols.contains(code))
                .map(functions::col).collect(Collectors.toList());
        ncols.add(functions.expr(String.format(format, code, dto.getSourcePattern(), dto.getTargetPattern())).as(code));
        if (!ncols.isEmpty()) {
            dataset = dataset.withColumn(code,
                    functions.concat((Seq) wrapRefArray(ncols.toArray())));
        }
        return dataset;
    }

    private Dataset<Row> colsArab2Chinese(CleanRuleDTO rule, Dataset<Row> dataset) {
        String rstr = JSON.toJSONString(rule.getData());
        Arab2ChineseDTO dto = JSON.parseObject(rstr, Arab2ChineseDTO.class);
        if (dto == null || StringUtils.isBlank(dto.getCode()) || StringUtils.isBlank(dto.getType())
                || StringUtils.isBlank(dto.getMode())) {
            logger.warn("数据清洗 阿拉伯中文数字转换的规则配置为空：{}", rstr);
            return dataset;
        }
        String code = dto.getCode();
        ConvertTypeEnum type = ConvertTypeEnum.getEnumByEnName(dto.getType());
        ConvertModeEnum mode = ConvertModeEnum.getEnumByEnName(dto.getMode());
        if (type == null || mode == null || !type.getModes().contains(mode)) {
            logger.warn("数据清洗 阿拉伯中文数字转换规则配置的转换类型暂不支持：{}", code, rstr);
            return dataset;
        }
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        if (!cols.contains(code)) {
            logger.warn("数据清洗 阿拉伯中文数字转换规则配置的字段{}在数据源列集中不存在：{}", code, rstr);
            return dataset;
        }
        int mv = mode.getCode() & 0xFFFF;
        List<org.apache.spark.sql.Column> ncols = cols.stream().filter(it -> !cols.contains(code))
                .map(functions::col).collect(Collectors.toList());
        ncols.add(functions.expr(String.format(ConvertTypeEnum.ARAB.equals(type) ? c2aFormat : a2cFormat, code, mv)).as(code));
        if (!ncols.isEmpty()) {
            dataset = dataset.withColumn(code,
                    functions.concat((Seq) wrapRefArray(ncols.toArray())));
        }
        return dataset;
    }

    private Dataset<Row> colsIDCardConvert(CleanRuleDTO rule, Dataset<Row> dataset) {
        String rstr = JSON.toJSONString(rule.getData());
        IDCardDTO dto = JSON.parseObject(rstr, IDCardDTO.class);
        if (dto == null || dto.getCodes() == null || dto.getCodes().isEmpty() || dto.getType() != 0) {
            logger.warn("数据清洗 身份证转换的规则配置不完整：{}", rstr);
            return dataset;
        }
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<String> list = dto.getCodes().stream().filter(it -> StringUtils.isNotBlank(it) && cols.contains(it))
                .collect(Collectors.toList());
        if (list.isEmpty()) {
            logger.warn("数据清洗 身份证转换规则配置字段跟实际数据字段不符：{}", rstr);
            return dataset;
        }
        List<org.apache.spark.sql.Column> ncols = cols.stream().map(it -> !list.contains(it) ? col(it) :
                        functions.callUDF("generateCardId", col(it).cast(DataTypes.StringType)).as(it))
                .collect(Collectors.toList());
        dataset = dataset.select((Seq) wrapRefArray(ncols.toArray()));
        return dataset;
    }

    private Dataset<Row> colsBlankReplace(CleanRuleDTO rule, Dataset<Row> dataset) {
        String rstr = JSON.toJSONString(rule.getData());
        RplaceBlankDTO dto = JSON.parseObject(rstr, RplaceBlankDTO.class);
        if (dto == null || dto.getCodes() == null || dto.getCodes().isEmpty() || StringUtils.isEmpty(dto.getValue())) {
            logger.warn("数据清洗 空值替换为指定值的规则配置不完整：{}", rstr);
            return dataset;
        }
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<String> list = dto.getCodes().stream().filter(it -> StringUtils.isNotBlank(it) && cols.contains(it))
                .collect(Collectors.toList());
        if (list.isEmpty()) {
            logger.warn("数据清洗 空值替换为指定值规则配置字段跟实际数据字段不符：{}", rstr);
            return dataset;
        }
        List<org.apache.spark.sql.Column> ncols = cols.stream().map(it -> !list.contains(it) ? col(it) :
                        functions.expr(String.format(e2sFormat, it, dto.getValue())).as(it))
                .collect(Collectors.toList());
        dataset = dataset.select((Seq) wrapRefArray(ncols.toArray()));
        return dataset;
    }

    private Dataset<Row> colsNullReplace(CleanRuleDTO rule, Dataset<Row> dataset) {
        String rstr = JSON.toJSONString(rule.getData());
        RplaceBlankDTO dto = JSON.parseObject(rstr, RplaceBlankDTO.class);
        if (dto == null || dto.getCodes() == null || dto.getCodes().isEmpty() || StringUtils.isEmpty(dto.getValue())) {
            logger.warn("数据清洗 NULL替换为指定值的规则配置不完整：{}", rstr);
            return dataset;
        }
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<String> list = dto.getCodes().stream().filter(it -> StringUtils.isNotBlank(it) && cols.contains(it))
                .collect(Collectors.toList());
        if (list.isEmpty()) {
            logger.warn("数据清洗 NULL替换为指定值规则配置字段跟实际数据字段不符：{}", rstr);
            return dataset;
        }
        List<org.apache.spark.sql.Column> ncols = cols.stream().map(it -> !list.contains(it) ? col(it) :
                        functions.expr(String.format(n2sFormat, it, dto.getValue())).as(it))
                .collect(Collectors.toList());
        dataset = dataset.select((Seq) wrapRefArray(ncols.toArray()));
        return dataset;
    }

    private Dataset<Row> colsReplaceNull(CleanRuleDTO rule, Dataset<Row> dataset) {
        String rstr = JSON.toJSONString(rule.getData());
        RplaceBlankDTO dto = JSON.parseObject(rstr, RplaceBlankDTO.class);
        if (dto == null || dto.getCodes() == null || dto.getCodes().isEmpty() || StringUtils.isEmpty(dto.getValue())) {
            logger.warn("数据清洗 指定值替换为NULL的规则配置不完整：{}", rstr);
            return dataset;
        }
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<String> list = dto.getCodes().stream().filter(it -> StringUtils.isNotBlank(it) && cols.contains(it))
                .collect(Collectors.toList());
        if (list.isEmpty()) {
            logger.warn("数据清洗 指定值替换为NULL规则配置字段跟实际数据字段不符：{}", rstr);
            return dataset;
        }
        List<org.apache.spark.sql.Column> ncols = cols.stream().map(it -> !list.contains(it) ? col(it) :
                        functions.expr(String.format(s2nFormat, it, dto.getValue())).as(it))
                .collect(Collectors.toList());
        dataset = dataset.select((Seq) wrapRefArray(ncols.toArray()));
        return dataset;
    }

    private Dataset<Row> colsTruncation(CleanRuleDTO rule, Dataset<Row> dataset) {
        String rstr = JSON.toJSONString(rule.getData());
        TruncationDTO dto = JSON.parseObject(rstr, TruncationDTO.class);
        if (dto == null || dto.getCols() == null || dto.getCols().isEmpty()) {
            logger.warn("数据清洗 舍位的规则配置不完整：{}", rstr);
            return dataset;
        }
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<TrunCol> truns = dto.getCols().stream().filter(it -> it != null && cols.contains(it.getCode())
                && it.getDivisor() != 0 && it.getPoint() >= 0).collect(Collectors.toList());
        if (truns.isEmpty()) {
            logger.warn("数据清洗 舍位的规则配置的字段跟实际数据字段不符：{}", rstr);
            return dataset;
        }
        List<org.apache.spark.sql.Column> ncols = new ArrayList<>();
        List<String> list = new ArrayList<>();
        truns.stream().forEach(it -> {
            ncols.add(functions.expr(String.format(crFormat, it.getCode(), it.getDivisor(), it.getPoint())).as(it.getCode()));
            list.add(it.getCode());
        });
        cols.stream().filter(it -> !list.contains(it)).forEach(col -> ncols.add(col(col)));
        dataset = dataset.select((Seq) wrapRefArray(ncols.toArray()));
        return dataset;
    }

    private Dataset<Row> colsRemoveStr(CleanRuleDTO rule, Dataset<Row> dataset) {
        String rstr = JSON.toJSONString(rule.getData());
        RemoveStrDTO dto = JSON.parseObject(rstr, RemoveStrDTO.class);
        if (dto == null || StringUtils.isBlank(dto.getCode()) || StringUtils.isEmpty(dto.getSourceStr())) {
            logger.warn("数据清洗 删除字符（串）的规则配置不完整：{}", rstr);
            return dataset;
        }
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        String code = dto.getCode();
        if (!cols.contains(code)) {
            logger.warn("数据清洗 删除字符（串）规则配置的替换字段跟实际数据字段不符：{}", rstr);
            return dataset;
        }
        List<org.apache.spark.sql.Column> ncols = cols.stream().map(it -> !cols.contains(it) ? col(it) :
                        functions.expr(String.format(rmFormat, it, dto.getSourceStr(), dto.getPos())).as(it))
                .collect(Collectors.toList());
        dataset = dataset.select((Seq) wrapRefArray(ncols.toArray()));
        return dataset;
    }

    private Dataset<Row> colsRemoveStrCount(CleanRuleDTO rule, Dataset<Row> dataset) {
        String rstr = JSON.toJSONString(rule.getData());
        RemoveCountStrDTO dto = JSON.parseObject(rstr, RemoveCountStrDTO.class);
        if (dto == null || dto.getCodes() == null || dto.getCodes().isEmpty() || dto.getLength() < 1) {
            logger.warn("数据清洗 删除指定数目的字符的规则配置不完整：{}", rstr);
            return dataset;
        }
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<String> list = dto.getCodes().stream().filter(it -> StringUtils.isNotBlank(it) && cols.contains(it))
                .collect(Collectors.toList());
        if (list.isEmpty()) {
            logger.warn("数据清洗 删除指定数目的字符规则配置的替换字段跟实际数据字段不符：{}", rstr);
            return dataset;
        }
        List<org.apache.spark.sql.Column> ncols = cols.stream().map(it -> !list.contains(it) ? col(it) :
                        functions.expr(String.format(rmaFormat, it, dto.getLength(), dto.getPos())).as(it))
                .collect(Collectors.toList());
        dataset = dataset.select((Seq) wrapRefArray(ncols.toArray()));
        return dataset;
    }

    private Dataset<Row> colsRemoveStrIndex(CleanRuleDTO rule, Dataset<Row> dataset) {
        String rstr = JSON.toJSONString(rule.getData());
        RemoveIndexStrDTO dto = JSON.parseObject(rstr, RemoveIndexStrDTO.class);
        if (dto == null || StringUtils.isEmpty(dto.getSourceStr()) || dto.getCodes() == null || dto.getCodes().isEmpty() ||
                dto.getLength() < 1) {
            logger.warn("数据清洗 删除字符串前后字符的规则配置不完整：{}", rstr);
            return dataset;
        }
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<String> list = dto.getCodes().stream().filter(it -> StringUtils.isNotBlank(it) && cols.contains(it))
                .collect(Collectors.toList());
        if (list.isEmpty()) {
            logger.warn("数据清洗 删除字符串前后字符规则配置的替换字段跟实际数据字段不符：{}", rstr);
            return dataset;
        }
        List<org.apache.spark.sql.Column> ncols = cols.stream().map(it -> !list.contains(it) ? col(it) :
                        functions.expr(String.format(rmpFormat, it, dto.getSourceStr(), dto.getLength(), dto.getPos())).as(it))
                .collect(Collectors.toList());
        dataset = dataset.select((Seq) wrapRefArray(ncols.toArray()));
        return dataset;
    }

    private Dataset<Row> colsAppendStr(CleanRuleDTO rule, Dataset<Row> dataset) {
        String rstr = JSON.toJSONString(rule.getData());
        StrIndexAppendDTO dto = JSON.parseObject(rstr, StrIndexAppendDTO.class);
        if (dto == null || StringUtils.isEmpty(dto.getStr()) || dto.getCodes() == null || dto.getCodes().isEmpty() ||
                StringUtils.isBlank(dto.getSourceStr())) {
            logger.warn("数据清洗 在字符串前后添加字符（串）的规则配置不完整：{}", rstr);
            return dataset;
        }
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<String> list = dto.getCodes().stream().filter(it -> StringUtils.isNotBlank(it) && cols.contains(it))
                .collect(Collectors.toList());
        if (list.isEmpty()) {
            logger.warn("数据清洗 在字符串前后添加字符（串）规则配置的替换字段跟实际数据字段不符：{}", rstr);
            return dataset;
        }
        List<org.apache.spark.sql.Column> ncols = cols.stream().map(it -> !list.contains(it) ? col(it) :
                        functions.expr(String.format(fappFormat, it, dto.getStr(), dto.getSourceStr(), dto.getPos())).as(it))
                .collect(Collectors.toList());
        dataset = dataset.select((Seq) wrapRefArray(ncols.toArray()));
        return dataset;
    }

    private Dataset<Row> colsAppendStrIndex(CleanRuleDTO rule, Dataset<Row> dataset) {
        String rstr = JSON.toJSONString(rule.getData());
        AppendStrDTO dto = JSON.parseObject(rstr, AppendStrDTO.class);
        if (dto == null || StringUtils.isBlank(dto.getStr()) || dto.getCodes() == null || dto.getCodes().isEmpty()) {
            logger.warn("数据清洗 指定位置追加字符（串）的规则配置不完整：{}", rstr);
            return dataset;
        }
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<String> list = dto.getCodes().stream().filter(it -> StringUtils.isNotBlank(it) && cols.contains(it))
                .collect(Collectors.toList());
        if (list.isEmpty()) {
            logger.warn("数据清洗 指定位置追加字符（串）规则配置的替换字段跟实际数据字段不符：{}", rstr);
            return dataset;
        }
        List<org.apache.spark.sql.Column> ncols = cols.stream().map(it -> !list.contains(it) ? col(it) :
                        functions.expr(String.format(appendFormat, it, dto.getStr(), dto.getPos())).as(it))
                .collect(Collectors.toList());
        dataset = dataset.select((Seq) wrapRefArray(ncols.toArray()));
        return dataset;
    }

    private Dataset<Row> colsStrReplace(CleanRuleDTO rule, Dataset<Row> dataset) {
        String rstr = JSON.toJSONString(rule.getData());
        StrReplaceDTO dto = JSON.parseObject(rstr, StrReplaceDTO.class);
        if (dto == null || StringUtils.isBlank(dto.getSourceStr()) || dto.getCodes() == null || dto.getCodes().isEmpty()) {
            logger.warn("数据清洗 替换字符串的规则配置不完整：{}", rstr);
            return dataset;
        }
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<String> list = dto.getCodes().stream().filter(it -> StringUtils.isNotBlank(it) && cols.contains(it))
                .collect(Collectors.toList());
        if (list.isEmpty()) {
            logger.warn("数据清洗 替换字符串规则配置的替换字段跟实际数据字段不符：{}", rstr);
            return dataset;
        }
        String tar = dto.getTargetStr() == null ? "" : dto.getTargetStr();
        List<org.apache.spark.sql.Column> ncols = cols.stream().map(it -> !list.contains(it) ? col(it) :
                        functions.expr(String.format(repFormat, it, dto.getSourceStr(), tar, dto.getPos())).as(it))
                .collect(Collectors.toList());
        dataset = dataset.select((Seq) wrapRefArray(ncols.toArray()));
        return dataset;
    }

    private Dataset<Row> colsStrFill(CleanRuleDTO rule, Dataset<Row> dataset) {
        String rstr = JSON.toJSONString(rule.getData());
        StrFillDTO dto = JSON.parseObject(rstr, StrFillDTO.class);
        if (dto == null || StringUtils.isBlank(dto.getStr()) || dto.getCodes() == null || dto.getCodes().isEmpty()
                || dto.getLength() < 1) {
            logger.warn("数据清洗 字符串填充的规则配置不完整：{}", rstr);
            return dataset;
        }
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        List<String> list = dto.getCodes().stream().filter(it -> StringUtils.isNotBlank(it) && cols.contains(it))
                .collect(Collectors.toList());
        if (list.isEmpty()) {
            logger.warn("数据清洗 字符串填充规则配置的填充字段跟实际数据字段不符：{}", rstr);
            return dataset;
        }
        List<org.apache.spark.sql.Column> ncols = cols.stream().map(it -> !list.contains(it) ? col(it) :
                        functions.expr(String.format(fillFormat, it, dto.getStr(), dto.getPos(), dto.getLength())).as(it))
                .collect(Collectors.toList());
        dataset = dataset.select((Seq) wrapRefArray(ncols.toArray()));
        return dataset;
    }

    private Dataset<Row> colsMerge(CleanRuleDTO rule, Dataset<Row> dataset) {
        String rstr = JSON.toJSONString(rule.getData());
        MergeDTO dto = JSON.parseObject(rstr, MergeDTO.class);
        if (dto == null || StringUtils.isBlank(dto.getCode()) || dto.getCols() == null || dto.getCols().isEmpty()) {
            logger.warn("数据清洗 多字段合并的规则配置为空：{}", rstr);
            return dataset;
        }
        String code = dto.getCode();
        List<Concat> list = dto.getCols().stream().filter(it -> StringUtils.isNotBlank(it.getCode())).collect(Collectors.toList());
        if (list.size() <= 1) {
            logger.warn("数据清洗 多字段合并规则配置的被合并字段至少包含两个：{}", rstr);
            return dataset;
        }
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        int len = list.size();
        List<org.apache.spark.sql.Column> ncols = new ArrayList<>();
        for (int i = 0; i < len; i++) {
            Concat cc = list.get(i);
            if (!cols.contains(cc.getCode())) {
                logger.warn("数据清洗 多字段合并规则配置的合并字段{}在数据源列集中不存在：{}", cc.getCode(), rstr);
                continue;
            }
            String sign = cc.getConnSign();
            String ccode = cc.getCode();
            ncols.add(StringUtils.isEmpty(sign) ? col(ccode) : functions.expr(String.format(appendFormat, ccode, sign, 1)));
        }
        if (!ncols.isEmpty()) {
            dataset = dataset.withColumn(code,
                    functions.concat((Seq) wrapRefArray(ncols.toArray())));
        }
        return dataset;
    }

    private Dataset<Row> colsSplit(CleanRuleDTO rule, Dataset<Row> dataset) {
        String rstr = JSON.toJSONString(rule.getData());
        SplitDTO dto = JSON.parseObject(rstr, SplitDTO.class);
        if (dto == null || StringUtils.isBlank(dto.getCode()) || StringUtils.isEmpty(dto.getSeparator())
                || dto.getCols() == null || dto.getCols().isEmpty()) {
            logger.warn("数据清洗 字符串拆分的规则配置为空：{}", rstr);
            return dataset;
        }
        String code = dto.getCode();
        String separator = dto.getSeparator();
        List<String> list = dto.getCols().stream().filter(it -> StringUtils.isNotBlank(it.getCode()))
                .map(Column::getCode).collect(Collectors.toList());
        if (list.isEmpty()) {
            logger.warn("数据清洗 字符串拆分的规则配置的拆分后的列为空：{}", rstr);
            return dataset;
        }
        List<String> cols = new ArrayList<>(Arrays.asList(dataset.columns()));
        if (!cols.contains(code)) {
            logger.warn("数据清洗 字符串拆分的字段在数据源列集中不存在：{}", rstr);
            return dataset;
        }
        String splitCol = "split_col_" + code;
        List<org.apache.spark.sql.Column> ncols = cols.stream().map(it -> col(it)).collect(Collectors.toList());
        list.forEach(it -> ncols.add(col(splitCol).getItem(list.indexOf(it)).as(it)));
        dataset = dataset.withColumn(splitCol, functions.split(col(code), separator))
                .select((Seq) wrapRefArray(ncols.toArray()));
        return dataset;
    }

    /*public static void main(String[] args) {
        Map<String, Object> map = new HashMap<>();
        map.putAll(ImmutableMap.of("A", 1, "B", "vb", "C", "c1,c2,c3", "D", "2010-12-22"));
        Map<String, Object> map1 = new HashMap<>();
        map1.putAll(ImmutableMap.of("A", 2, "B", "134", "C", "c2", "D", "2021-12-23"));
//        map1.put("D", null);
        Map<String, Object> map2 = new HashMap<>();
        map2.putAll(ImmutableMap.of("A", 3, "B", "132.465", "C", "c3,c4", "D", ""));
        Map<String, Object> map3 = new HashMap<>();
        map3.putAll(ImmutableMap.of("A", 4, "B", "32.51", "C", "c4", "D", "2021-12-23"));
//        map3.put("D", null);
        Map<String, Object> map4 = new HashMap<>();
        map4.putAll(ImmutableMap.of("A", 5, "B", "1231.45", "C", "c4", "D", ""));
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
        sparkSession.sqlContext().udf().register("filedAppend", new FiledAppendIndexUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("filedFill", new FiledFillUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("filedReplace", new FiledReplaceUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("filedCarry", new FiledCarryUdf(), DataTypes.DoubleType);
        sparkSession.sqlContext().udf().register("deleteBlank", new FiledDeleteBlankUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("some2Null", new Some2NullUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("null2Some", new Null2SomeUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("generateCardId", new GenerateIdCardUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("arabicToCn", new ArabicToCnUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("cnToArabic", new CnToArabicUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("datetimeFormat", new DateTimeTransferUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("dateFormat", new DateTransferUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("timeFormat", new TimeTransferUdf(), DataTypes.StringType);
        // 注册成表
        Dataset<Row> dataset = sparkSession.read().json(javaRDD);
        dataset.show();
        ConvertTypeEnum type = ConvertTypeEnum.CHINESE;
        int mode = type.getModes().get(1).getCode() & 0xFFFF;
        List<org.apache.spark.sql.Column> ncols = Arrays.stream(dataset.columns()).map(functions::col).collect(Collectors.toList());
//        ncols.add(functions.expr(String.format(dateFormat, "D", "yyyy-MM-dd", "yyyy/MM/dd")));
        ncols.add(lit(null).as("DD"));
        dataset.select((Seq) wrapRefArray(ncols.toArray())).show();
//        dataset.withColumn("DF", functions.expr(String.format(ConvertTypeEnum.ARAB.equals(type)
//                ? c2aFormat : a2cFormat, "B", mode))).show();
     *//*   List<org.apache.spark.sql.Column> ncols = new ArrayList<>();
        ncols.add(col("A"));
        ncols.add(col("B"));
        ncols.add(functions.expr(String.format(repFormat, "B", "b", "@", 2)).as("BB"));
        ncols.add(functions.expr(String.format(repFormat, "C", "c", "A", 0)).as("C"));
        ncols.add(functions.expr(String.format(repFormat, "D", "01", "59", 1)).as("C"));
//        ncols.add(functions.expr(String.format(fillFormat, "C", "000", 1, 10)).as("C"));
//        ncols.add(functions.expr(String.format(fillFormat, "D", "2021-12-24", 0, 20)).as("D"));
        dataset.select((Seq) wrapRefArray(ncols.toArray())).show();*//*
//        ncols.add(functions.expr(String.format(appendFormat, "B", "=", 1)));
//        ncols.add(functions.expr(String.format(appendFormat, "C", ";", 1)));
//        ncols.add(col("D"));
//        dataset.withColumn("FF",
//                functions.concat((Seq) wrapRefArray(ncols.toArray()))).show();
       *//* dataset = dataset.withColumn("DD", functions.concat_ws("-", col("B"), col("C")))
                .withColumn("DD", functions.concat_ws(";", col("DD"), col("D")))
                .withColumn("A", functions.expr(String.format(appendFormat, "A", "123", 0)))
                .withColumn("hh", lit(null).cast(DataTypes.StringType));*//*
     *//*  List<org.apache.spark.sql.Column> cols = new ArrayList<>();
          cols.add(col("A"));
          cols.add(col("B"));
          cols.add(col("C"));
          cols.add(col("D"));
          cols.add(col("FF").getItem(0).as("C1"));
          cols.add(col("FF").getItem(1).as("C2"));
          cols.add(col("FF").getItem(2).as("C3"));
          cols.add(col("FF").getItem(3).as("C4"));
          dataset = dataset.withColumn("FF", functions.split(col("C"), ","))
                  .select(JavaConverters.asScalaIteratorConverter(cols.iterator()).asScala().toSeq());*//*
        sparkSession.close();

    }*/
}
