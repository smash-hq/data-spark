package com.wk.data.spark.service.mdm.executor.impl;

import com.wk.data.etl.facade.catalog.api.CatalogFacade;
import com.wk.data.etl.facade.catalog.vo.CatalogVO;
import com.wk.data.etl.facade.mdm.api.MdmFacade;
import com.wk.data.etl.facade.mdm.vo.JoinRelationVO;
import com.wk.data.etl.facade.mdm.vo.MdmCfgVO;
import com.wk.data.etl.facade.mdm.vo.SourceRelationVO;
import com.wk.data.etl.facade.mdm.vo.TableVO;
import com.wk.data.etl.facade.table.ColInfo;
import com.wk.data.spark.infrastructure.master.database.po.MdmCfgDO;
import com.wk.data.spark.infrastructure.mysqldb.dao.CatalogDao;
import com.wk.data.spark.service.mdm.executor.MdmTempService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.lang.Boolean.TRUE;

/**
 * @Author: smash_hq
 * @Date: 2021/12/14 9:56
 * @Description: 临时表实现方案
 * @Version v1.0
 */
@Component
public class MdmTempServiceImpl implements MdmTempService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MdmTempServiceImpl.class);

    @Autowired
    private SparkSession spark;
    @Autowired
    private MdmFacade mdmFacade;
    @Autowired
    private CatalogFacade catalogFacade;
    @Autowired
    private CatalogDao mySqlDao;

    @Value("${mysql.url}")
    private String url;
    @Value("${mysql.username}")
    private String user;
    @Value("${mysql.password}")
    private String password;
    @Value("${mysql.driver}")
    private String driver;

    private String databaseCode;
    private String tableCode;
    private int keysSize;
    private Connection connection;

    public static void main(String[] args) {
        List<String> list1 = Arrays.asList("a", "f", "b", "c");
        List<String> list2 = Arrays.asList("d", "b", "c");
        Collection<String> collection = CollectionUtils.retainAll(list1, list2);
        System.out.println(collection);
    }


    @Override
    public void mdmTemp(String... args) throws Exception {
        long beginTime = System.currentTimeMillis();
        // 1 读取配置信息
        MdmCfgDO mdmCfgDO = new MdmCfgDO();

        // 2 获取关联关系
        MdmCfgVO vo = mdmFacade.getSingle("61b15a0b49b3f54d6da66fc4").getData();
        TableVO master = vo.getMasterDataTable();
        this.tableCode = master.getTableCode();
        this.databaseCode = master.getDatabase();
        JoinRelationVO joinRelationVO = vo.getJoinRelation();
        List<SourceRelationVO> sourceRelationVO = vo.getSourceRelation();

        // 3 创建临时表
        createTempView(spark, joinRelationVO);

        // 4 执行操作
        String sql = sql(joinRelationVO, sourceRelationVO);
        Dataset<Row> dataset = spark.sql(sql);

        // 5 主数据表保存
        Integer updateMode = vo.getUpdateMode();
        switch (updateMode) {
            case 0:
                writer(dataset, SaveMode.Overwrite);
                break;
            case 1:
                writer(dataset, SaveMode.Append);
                break;
            case 2:
                updateWriter(dataset, master);
                break;
            default:
                break;
        }

        // 6 关闭
        connection.close();
        dropTempView(spark, joinRelationVO);
        spark.close();
        System.out.println("========" + (System.currentTimeMillis() - beginTime) + "\n");
    }

    private String assembleCreateSQL(CatalogVO catalogVO, String tempTableCode) {
        List<ColInfo> colInfos = catalogVO.getColInfo();
        StringBuilder sb1 = new StringBuilder();
        sb1.append("create table ").append(tempTableCode).append("(");
        StringBuilder sb2 = new StringBuilder();
        Integer zoneInfo = catalogVO.getZoneInfo();
        List<String> primaryKeys = new ArrayList<>();
        for (int i = 0; i < colInfos.size(); i++) {
            ColInfo col = colInfos.get(i);
            String colCode = col.getCode();
            String colType = col.getType();
            String colComment = col.getComment();
            Integer colLength = col.getLength();
            Integer colDecimalLength = col.getDecimalLength();
            Boolean colIsNull = col.getIsNotNull();
            Boolean colIsPrimaryKey = col.getIsPrimaryKey();
            if ("varchar".equals(colType)) {
                sb2.append(colCode).append(" ").append(colType).append("(").append(colLength).append(")");
            }
            if ("decimal".equals(colType) && colDecimalLength != null && colDecimalLength != 0) {
                sb2.append(colCode).append(" ").append(colType).append("(").append(colLength).append(",").append(colDecimalLength).append(")");
            }
            if ("date".equals(colType) || "timestamp".equals(colType) || "text".equals(colType) ||
                    "integer".equals(colType) || "bigint".equals(colType)) {
                sb2.append(colCode).append(" ").append(colType);
            }
            if (TRUE.equals(colIsNull)) {
                if ("timestamp".equals(colType)) {
                    sb2.append(" NOT NULL DEFAULT CURRENT_TIMESTAMP");
                } else {
                    sb2.append(" NOT NULL");
                }
            } else {
                sb2.append(" NULL");
            }
            if (StringUtils.isNotBlank(colComment)) {
                sb2.append(" COMMENT '").append(colComment).append("' ");
            }
            if (TRUE.equals(colIsPrimaryKey)) {
                primaryKeys.add(col.getCode());
            }
            sb2.append(i == colInfos.size() - 1 ? ", system_insert_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" : ", ");
        }
        // 是否增加分区信息逻辑判断
        sb1.append(sb2);
        if (!primaryKeys.isEmpty()) {
            String keys = String.join(",", primaryKeys);
            sb1.append(",").append("PRIMARY KEY (").append(keys).append(",system_insert_time)");
        }
        sb1.append(")");
        sb1.append("ENGINE=InnoDB DEFAULT CHARSET=utf8");
        return sb1.toString();
    }

    private void updateWriter(Dataset<Row> dataset, TableVO master) {
        try {
            String id = master.getId();
            CatalogVO catalogVO = catalogFacade.getSingle(id).getData();
            // 1 连接数据库
            this.connection = mySqlDao.connMysql(databaseCode);

            // 2 创建临时表以做更新依据表并填充数据
            String tempTableCode = "tableCode" + "_temp" + System.currentTimeMillis();
            String createTempTable = assembleCreateSQL(catalogVO, tempTableCode);
            connection.prepareStatement(createTempTable).execute();
            writerByCode(dataset, SaveMode.Append, tempTableCode);

            String updateSql = assembleUpdateSql(catalogVO, tempTableCode);
            String replaceSql = assembleReplaceSql(catalogVO, tempTableCode);
            String insertSql = assembleInsertSql(catalogVO, tempTableCode);
            connection.prepareStatement(updateSql).execute();
            connection.prepareStatement(insertSql).execute();

            // 删除临时表
            connection.prepareStatement("DROP TABLE " + tempTableCode).execute();

        } catch (SQLException e) {
            LOGGER.error("", e);
        }
    }

    private String assembleInsertSql(CatalogVO catalogVO, String tempTableCode) {
        StringBuilder sb = new StringBuilder();
        List<ColInfo> colInfos = catalogVO.getColInfo();
        int colSize = colInfos.size();
        sb.append("INSERT INTO ").append(tableCode).append("(");
        StringBuilder sonSb = new StringBuilder();
        StringBuilder filedSb = new StringBuilder();
        List<String> primaryKeys = new ArrayList<>();
        for (int i = 0; i < colSize; i++) {
            if (Boolean.TRUE.equals(colInfos.get(i).getIsPrimaryKey())) {
                primaryKeys.add(colInfos.get(i).getCode());
            }
            filedSb.append(colInfos.get(i).getCode());
            sonSb.append("t0.").append(colInfos.get(i).getCode());
            if (i == colSize - 1) {
                break;
            }
            filedSb.append(",");
            sonSb.append(",");
        }
        sb.append(filedSb);
        sb.append(") SELECT ").append(sonSb).append(" FROM ").append(tempTableCode).append(" t0");
        sb.append(" WHERE NOT EXISTS ( SELECT * FROM ").append(tableCode).append(" t1 WHERE ");
        for (int i = 0; i < primaryKeys.size(); i++) {
            sb.append("t0.").append(primaryKeys.get(i)).append(" = ").append("t1.").append(primaryKeys.get(i));
            if (i == primaryKeys.size() - 1) {
                break;
            }
            sb.append(" AND ");
        }
        return sb.append(")").toString();
    }

    private String assembleReplaceSql(CatalogVO catalogVO, String tempTableCode) {
        StringBuilder sb = new StringBuilder();
        List<ColInfo> colInfos = catalogVO.getColInfo();
        int colSize = colInfos.size();
        sb.append("REPLACE INTO ").append(tableCode).append("(");
        StringBuilder sonSb = new StringBuilder();
        StringBuilder filedSb = new StringBuilder();
        List<String> primaryKeys = new ArrayList<>();
        for (int i = 0; i < colSize; i++) {
            if (Boolean.TRUE.equals(colInfos.get(i).getIsPrimaryKey())) {
                primaryKeys.add(colInfos.get(i).getCode());
            }
            filedSb.append(colInfos.get(i).getCode());
            sonSb.append("t0.").append(colInfos.get(i).getCode());
            if (i == colSize - 1) {
                break;
            }
            filedSb.append(",");
            sonSb.append(",");
        }
        sb.append(filedSb);
        sb.append(") SELECT ").append(sonSb).append(" FROM ").append(tempTableCode).append(" t0");
        sb.append(" LEFT JOIN ").append(tableCode).append(" t1 ON ");
        for (int i = 0; i < primaryKeys.size(); i++) {
            sb.append("t0.").append(primaryKeys.get(i)).append(" = ").append("t1.").append(primaryKeys.get(i));
            if (i == primaryKeys.size() - 1) {
                break;
            }
            sb.append(" AND ");
        }
        return sb.toString();
    }

    private String assembleUpdateSql(CatalogVO catalogVO, String tempTableCode) {
        StringBuilder sb = new StringBuilder();
        List<ColInfo> colInfos = catalogVO.getColInfo();
        int colSize = colInfos.size();
        sb.append("UPDATE ").append(tableCode).append(" , ").append(tempTableCode).append(" SET ");
        List<String> primaryKeys = new ArrayList<>();
        for (int i = 0; i < colSize; i++) {
            String code = colInfos.get(i).getCode();
            sb.append(tableCode).append(".").append(code).append(" = ")
                    .append(tempTableCode).append(".").append(code);

            if (Boolean.TRUE.equals(colInfos.get(i).getIsPrimaryKey())) {
                primaryKeys.add(code);
            }
            if (i == colSize - 1) {
                break;
            }
            sb.append(",");
        }
        sb.append(" WHERE ");
        for (int i = 0; i < primaryKeys.size(); i++) {
            String key = primaryKeys.get(i);
            sb.append(tableCode).append(".").append(key).append(" = ").append(tempTableCode).append(".").append(key);
            if (i == primaryKeys.size() - 1) {
                break;
            }
            sb.append(" AND ");
        }
        return sb.toString();
    }

    private void dropTempView(SparkSession spark, JoinRelationVO joinRelationVO) {
        joinRelationVO.getRightTables().forEach(it -> spark.catalog().dropTempView(it.getTableCode()));
        spark.catalog().dropTempView(joinRelationVO.getLeftTable().getTableCode());
    }

    private void createTempView(SparkSession spark, JoinRelationVO joinRelationVO) throws AnalysisException {
        TableVO leftTable = joinRelationVO.getLeftTable();
        List<TableVO> rightTables = joinRelationVO.getRightTables();
        Dataset<Row> left = reader(spark, leftTable);
        left.createTempView(leftTable.getTableCode());
        for (TableVO table : rightTables) {
            Dataset<Row> right = reader(spark, table);
            right.createTempView(table.getTableCode());
        }
    }

    private void writer(Dataset<Row> dataset, SaveMode mode) {
        dataset.write().mode(mode).format("jdbc")
                .option("url", String.format(url, databaseCode))
                .option("driver", driver)
                .option("user", user)
                .option("password", password)
                .option("dbTable", tableCode)
                .save();
    }

    private void writerByCode(Dataset<Row> dataset, SaveMode mode, String tableCode) {
        dataset.write().mode(mode).format("jdbc")
                .option("url", String.format(url, databaseCode))
                .option("driver", driver)
                .option("user", user)
                .option("password", password)
                .option("dbTable", tableCode)
                .save();
    }

    private Dataset<Row> reader(SparkSession spark, TableVO tableVO) {
        String db = tableVO.getDatabase();
        String table = tableVO.getTableCode();
        return spark.read().format("jdbc")
                .option("url", String.format(url, databaseCode))
                .option("driver", driver)
                .option("user", user)
                .option("password", password)
                .option("query", "select * from " + table)
                .load();

    }

    private String sql(JoinRelationVO tables, List<SourceRelationVO> cfgs) {
        String left = tables.getLeftTable().getTableCode();
        StringBuilder filedCfgs = new StringBuilder();
        for (int i = 0; i < cfgs.size(); i++) {
            SourceRelationVO it = cfgs.get(i);
            String firstTable = it.getFirstTable();
            String firstFiled = it.getFirstFiled();
            String secondTable = it.getSecondTable();
            String secondFiled = it.getSecondFiled();
            String thirdTable = it.getThirdTable();
            String thirdFiled = it.getThirdFiled();
            String master = it.getMasterCode();
            if (StringUtils.isNotBlank(secondTable) && StringUtils.isNotBlank(secondFiled)) {
                filedCfgs.append("CASE WHEN ").append(firstTable).append(".").append(firstFiled)
                        .append(" IS NULL AND ").append(secondTable).append(".").append(secondFiled)
                        .append(" IS NOT NULL ").append(" THEN ").append(secondTable).append(".").append(secondFiled)
                        .append(StringUtils.isBlank(thirdTable) && StringUtils.isBlank(thirdFiled) ? "" :
                                new StringBuilder().append(" WHEN ").append(firstTable).append(".").append(firstFiled).append(" IS NULL AND ")
                                        .append(secondTable).append(".").append(secondFiled).append(" IS NULL THEN ")
                                        .append(thirdTable).append(".").append(thirdFiled).toString())
                        .append(" ELSE ").append(firstTable).append(".").append(firstFiled).append(" END ");
            } else {
                filedCfgs.append(firstTable).append(".").append(firstFiled);
            }
            filedCfgs.append(" AS ").append(master).append(i == cfgs.size() - 1 ? " " : ",");
        }

        String leftSql = "SELECT " + filedCfgs + " FROM " + left + " AS " + left;
        StringBuilder rightSql = new StringBuilder();
        for (TableVO tableDO : tables.getRightTables()) {
            String leftFiled = tableDO.getLeftFiled();
            String right = tableDO.getTableCode();
            String rightFiled = tableDO.getRightFiled();
            rightSql.append(" LEFT JOIN (").append("SELECT * FROM ").append(right).append(" AS ").append(right).append(")").append(right).append(" ON ")
                    .append(left).append(".").append(leftFiled).append("=")
                    .append(right).append(".").append(rightFiled);
        }
        return leftSql + rightSql;
    }

}
