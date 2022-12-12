package com.wk.data.spark.service.mdm.executor.impl;

import com.google.common.collect.Lists;
import com.wk.data.etl.facade.catalog.api.CatalogFacade;
import com.wk.data.etl.facade.catalog.vo.CatalogVO;
import com.wk.data.etl.facade.mdm.api.MdmFacade;
import com.wk.data.etl.facade.mdm.vo.JoinRelationVO;
import com.wk.data.etl.facade.mdm.vo.MdmCfgVO;
import com.wk.data.etl.facade.mdm.vo.SourceRelationVO;
import com.wk.data.etl.facade.mdm.vo.TableVO;
import com.wk.data.etl.facade.table.ColInfo;
import com.wk.data.share.facade.subscribe.api.DataSubscribeFacade;
import com.wk.data.share.facade.subscribe.dto.CatalogInfoDTO;
import com.wk.data.spark.infrastructure.mysqldb.dao.CatalogDao;
import com.wk.data.spark.service.mdm.executor.MdmService;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @Author: smash_hq
 * @Date: 2021/12/3 10:50
 * @Description: 主数据管理
 * @Version v1.0
 */
@Component
public class MdmServiceImpl implements MdmService, Serializable, scala.Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MdmServiceImpl.class);
    public static final String REGEX = "-";
    public static final String COUNT_SQL = "SELECT COUNT(1) AS count FROM ";
    public static final String DOT = ",";

    @Autowired
    private SparkSession spark;
    @Autowired
    private MdmFacade mdmFacade;
    @Autowired
    private CatalogFacade catalogFacade;
    @Autowired
    private CatalogDao mySqlDao;
    @Autowired
    private DataSubscribeFacade dataSubscribeFacade;

    @Value("${mysql.url}")
    private String url;
    @Value("${mysql.username}")
    private String user;
    @Value("${mysql.password}")
    private String password;
    @Value("${mysql.driver}")
    private String driver;

    private String layerCode;
    private String id;
    private String tableId;
    private CatalogVO catalogVO;
    private String databaseCode;
    private String tableCode;
    private int updateMode;
    private int keysSize;
    private Connection connection;
    private long updateCount;
    private int dataSize = 0;
    private int count = 0;
    private int execId = 0;


    @Override
    public void masterDataManagement(String[] args) {
        // 1 读取配置信息
        this.id = args[0];
        this.execId = Integer.parseInt(args[1]);
        LOGGER.info("Spark 接收参数 id：{} ", id);
        LOGGER.info("Spark 接收参数 execId：{} ", execId);
        MdmCfgVO vo = mdmFacade.getSingle(id).getData();
        if (vo == null) {
            LOGGER.error("主数据任务不存在");
            return;
        }
        TableVO master = vo.getMasterDataTable();
        this.layerCode = master.getLayerCode();
        this.updateMode = vo.getUpdateMode();
        this.tableId = master.getId();
        this.tableCode = master.getTableCode();
        this.databaseCode = master.getDatabase();
        this.catalogVO = catalogFacade.getSingle(tableId).getData();

        // 2 获取关联关系
        JoinRelationVO joinRelationVO = vo.getJoinRelation();
        List<SourceRelationVO> sourceRelationVO = vo.getSourceRelation();

        String sql = sql(joinRelationVO, sourceRelationVO);
        try {
            // 3 创建临时表
            createTempView(spark, joinRelationVO);

            // 4 执行操作
            Dataset<Row> dataset = spark.sql(sql);
            dropTempView(spark, joinRelationVO);

            // 5 主数据表保存
            this.connection = mySqlDao.connMysql(databaseCode);
            this.count = queryCount();
            this.updateCount = dataset.count();
            if (updateCount <= 0) {
                LOGGER.warn("被写入表：{}，写入数据0条", tableCode);
                return;
            }
            // 先将时间存储起来，保证保存操作的时间在后
            CatalogInfoDTO catalogInfoDTO = CatalogInfoDTO.builder()
                    .layer(layerCode)
                    .dbCode(databaseCode)
                    .tableCode(tableCode)
                    .type(updateMode)
                    .date(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
                    .build();
            switch (updateMode) {
                case 0:
                    coverWriter(dataset);
                    break;
                case 1:
                    appendWriter(dataset);
                    break;
                case 2:
                    updateWriter(dataset);
                    break;
                default:
                    break;
            }
            this.dataSize = queryCount();
            // 再写入操作完成后再调用rpc进行更新
            // 6 调用rpc接口更新MdmCfgDO中更新数据量和数据量信息
            dataSubscribeFacade.sendDataChangeInfo(catalogInfoDTO);
            mdmFacade.setDataSize(id, dataSize);
            mdmFacade.updateRecord(vo.getScheduleId(), execId, (int) updateCount);
            LOGGER.warn("主数据任务执行成功，写入主数据表>> {} {} 条记录,【0-覆盖 1-追加 2-更新-->{}】", tableCode, updateCount, updateMode);
        } catch (SQLException | AnalysisException e) {
            LOGGER.error("主数据任务执行失败:{}", e.getMessage());
        } finally {
            try {
                // 7 关闭
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                LOGGER.error("database closing failed:", e);
            }
            spark.close();
        }
    }

    private int queryCount() throws SQLException {
        int i = 0;
        ResultSet countResultSet = connection.prepareStatement(COUNT_SQL + tableCode).executeQuery();
        while (countResultSet.next()) {
            i = Integer.parseInt(countResultSet.getString("count"));
        }
        return i;
    }

    private void updateWriter(Dataset<Row> dataset) throws SQLException {
        List<String> primaryKeys = new ArrayList<>();
        catalogVO.getColInfo().forEach(it -> {
            if (Boolean.TRUE.equals(it.getIsPrimaryKey())) {
                primaryKeys.add(it.getCode());
            }
        });
        this.keysSize = primaryKeys.size();

        // 如果写入表没有主键直接进行append操作
        if (keysSize == 0) {
            appendWriter(dataset);
            return;
        }

        // 1 如果为空则直接写入
        if (count == 0) {
            appendWriter(dataset);
            return;
        }

        // 2 找到重复主键
        String keys = String.join(DOT, primaryKeys);
        String execSql = "SELECT " + keys + " FROM " + tableCode;
        ResultSet res = connection.prepareStatement(execSql).executeQuery();
        List<String> oriList = extracted(primaryKeys, res);
        List<String> willList = extracted(primaryKeys, dataset);
        List<String> list = ListUtils.retainAll(oriList, willList);

        // 3 删除重复数据
        delLoop(primaryKeys, list);

        // 4 append
        appendWriter(dataset);
    }

    private void delLoop(List<String> primaryKeys, List<String> list) {
        ExecutorService delThread = new ThreadPoolExecutor(5, 10, 5, TimeUnit.MINUTES, new SynchronousQueue<>(), Executors.defaultThreadFactory());
        List<List<String>> pages = Lists.partition(list, 500);
        int threadCount = pages.size();
        AtomicInteger pageSize = new AtomicInteger(0);
        CountDownLatch downLatch = new CountDownLatch(10);
        int listSize = list.size();
        if (keysSize == 1) {
            if (listSize > 500) {
                for (int i = 0; i < 10; i++) {
                    delThread.execute(() -> {
                        while (pageSize.get() < threadCount) {
                            List<String> page = pages.get(pageSize.getAndIncrement());
                            extracted(primaryKeys, page);
                        }
                        downLatch.countDown();

                    });
                }
                try {
                    downLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                delThread.shutdown();
            }
        } else {
            for (String it : list) {
                StringBuilder sb = new StringBuilder().append("DELETE FROM ").append(tableCode).append(" WHERE ");
                String[] strs = it.split(REGEX);
                int size = strs.length;
                for (int i = 0; i < keysSize; i++) {
                    sb.append(primaryKeys.get(i)).append("='").append(strs[i]).append("'");
                    if (i == keysSize - 1) {
                        sb.append(";");
                        break;
                    } else {
                        sb.append(" AND ");
                    }
                }
                mySqlDao.delete(sb.toString());
            }
        }
    }

    private void extracted(List<String> primaryKeys, List<String> page) {
        StringBuilder sb = new StringBuilder().append("DELETE FROM ").append(tableCode).append(" WHERE ")
                .append(primaryKeys.get(0)).append(" IN (");
        int size = page.size();
        for (int j = 0; j < size; j++) {
            String it = page.get(j);
            sb.append("'").append(it).append("'");
            if (j == size - 1) {
                break;
            } else {
                sb.append(DOT);
            }
        }
        sb.append(")");
        mySqlDao.delete(sb.toString());
    }

    private List<String> extracted(List<String> primaryKeys, ResultSet res) throws SQLException {
        List<String> oriList = new ArrayList<>();
        while (res.next()) {
            List<String> sonList = new ArrayList<>();
            for (int i = 0; i < keysSize; i++) {
                sonList.add(res.getString(primaryKeys.get(i)));
            }
            oriList.add(String.join(REGEX, sonList));
        }
        return oriList;
    }

    private List<String> extracted(List<String> primaryKeys, Dataset<Row> dataset) {
        List<String> willList = new ArrayList<>();
        dataset.toJavaRDD().toLocalIterator().forEachRemaining(row -> {
            List<String> sonList = new ArrayList<>();
            for (int i = 0; i < keysSize; i++) {
                sonList.add(row.getAs(primaryKeys.get(i)).toString());
            }
            willList.add(String.join(REGEX, sonList));
        });
        return willList;
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

    private void appendWriter(Dataset<Row> dataset) {
        long beginTime = System.currentTimeMillis();
        dataset.write().mode(SaveMode.Append).format("jdbc")
                .option("url", String.format(url, databaseCode))
                .option("driver", driver)
                .option("user", user)
                .option("password", password)
                .option("dbTable", tableCode)
                .save();
        LOGGER.info("spark 执行写入时间 {} ms", (System.currentTimeMillis() - beginTime));
    }

    private void coverWriter(Dataset<Row> dataset) {
        long beginTime = System.currentTimeMillis();
        dataset.write().mode(SaveMode.Overwrite).format("jdbc")
                .option("url", String.format(url, databaseCode))
                .option("driver", driver)
                .option("truncate", true)
                .option("user", user)
                .option("password", password)
                .option("dbTable", tableCode)
                .save();
        LOGGER.info("spark 执行覆盖写入时间 {} ms", (System.currentTimeMillis() - beginTime));
    }

    private Dataset<Row> reader(SparkSession spark, TableVO tableVO) {
        String db = tableVO.getDatabase();
        String table = tableVO.getTableCode();
        return spark.read().format("jdbc")
                .option("url", String.format(url, databaseCode))
                .option("driver", driver)
                .option("user", user)
                .option("password", password)
                .option("dbtable", table)
                .load();

    }

    private String sql(JoinRelationVO tables, List<SourceRelationVO> cfgs) {
        String left = tables.getLeftTable().getTableCode();
        StringBuilder filedCfgs = new StringBuilder();
        StringBuilder filedSQL = new StringBuilder();
        for (int i = 0; i < cfgs.size(); i++) {
            SourceRelationVO it = cfgs.get(i);
            String firstTable = it.getFirstTable();
            String firstFiled = it.getFirstFiled();
            String secondTable = it.getSecondTable();
            String secondFiled = it.getSecondFiled();
            String thirdTable = it.getThirdTable();
            String thirdFiled = it.getThirdFiled();
            String master = it.getMasterCode();
            filedSQL.append(master).append(i == cfgs.size() - 1 ? "" : ",");
            if (StringUtils.isNotBlank(firstTable) && StringUtils.isNotBlank(firstFiled)) {
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
            } else {
                break;
            }
            filedCfgs.append(" AS ").append(master).append(i == cfgs.size() - 1 ? " " : DOT);
        }

        String leftSql = "SELECT " + filedCfgs + " FROM " + left + " AS " + left;
        StringBuilder rightSql = new StringBuilder();
        for (TableVO tableDO : tables.getRightTables()) {
            String leftFiled = tableDO.getLeftFiled();
            String right = tableDO.getTableCode();
            String rightFiled = tableDO.getRightFiled();
            rightSql.append(" LEFT JOIN (").append("SELECT * FROM ").append(right).append(" AS ").append(right).append(") AS ").append(right).append(" ON ")
                    .append(left).append(".").append(leftFiled).append("=")
                    .append(right).append(".").append(rightFiled);
        }
        String sql = leftSql + rightSql;
        List<ColInfo> colInfos = catalogVO.getColInfo().stream().filter(colInfo -> colInfo.getIsPrimaryKey() && !"id".equals(colInfo.getCode())).limit(1).collect(Collectors.toList());
        if (!colInfos.isEmpty()) {
            String primaryKey = colInfos.get(0).getCode();
            String rankSql = "SELECT " + filedSQL + ",ROW_NUMBER() OVER(PARTITION BY " + primaryKey + " ORDER BY " + primaryKey + ") AS row_number_rank from (" + sql + ")t0";
            return "SELECT " + filedSQL + " FROM (" + rankSql + ")t1 WHERE t1.row_number_rank = 1";
        }
        return sql;
    }
}
