package com.wk.data.spark.infrastructure.mysqldb.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.IsoFields;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

/**
 * @Author: smash_hq
 * @Date: 2021/7/26 17:59
 * @Description: mysql
 * @Version v1.0
 */
@Repository
public class CatalogDao {
    private static Logger logger = LoggerFactory.getLogger(CatalogDao.class);

    private Connection connection;

    @Value("${mysql.url}")
    private String url;
    @Value("${mysql.username}")
    private String user;
    @Value("${mysql.password}")
    private String password;
    @Value("${mysql.driver}")
    private String driver;


    /**
     * 连接 mysql 数据库
     *
     * @param databaseCode
     * @return
     */
    public Connection connMysql(String databaseCode) throws SQLException {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            logger.error("驱动设置失败：", e);
        }
        this.connection = DriverManager.getConnection(String.format(url, databaseCode), user, password);
        return connection;
    }

    /**
     * 单一查询语句预编译
     *
     * @param connection
     * @return
     */
    public PreparedStatement creatStatement(Connection connection, String sql) throws SQLException {
        return connection.prepareStatement(sql);
    }

    /**
     * 不需要 sql 的对象
     *
     * @param connection
     * @return
     */
    public Statement creatStatement(Connection connection) throws SQLException {
        return connection.createStatement();
    }

    /**
     * 新增编目库
     *
     * @param statement
     * @param databaseCode
     * @throws SQLException
     */
    public void addDatabase(Statement statement, String databaseCode) throws SQLException {
        statement.executeUpdate("CREATE database IF NOT EXISTS " + databaseCode + " DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci");
    }

    public void deleteDatabase(Statement statement, String databaseCode) throws SQLException {
        statement.execute("DROP DATABASE " + databaseCode);
    }

    /**
     * 新增编目表
     *
     * @param connection
     * @param sql
     * @return
     */
    public void addCatalog(Connection connection, String sql) throws SQLException {
        connection.createStatement().execute(sql);
    }


    /**
     * 验证编目表存在唯一性
     *
     * @param connection
     * @param catalogCode
     * @return
     */
    public Boolean verify(Connection connection, String catalogCode) throws SQLException {
        for (String code : catalogList(connection)) {
            if (code.equals(catalogCode)) {
                return FALSE;
            }
        }
        return TRUE;
    }

    /**
     * 列出该数据库下所有表
     *
     * @param connection
     * @return
     */
    public List<String> catalogList(Connection connection) throws SQLException {
        PreparedStatement statement1 = creatStatement(connection, "SHOW TABLES");
        ResultSet resultSet = statement1.executeQuery();
        List<String> list = new ArrayList();
        while (resultSet.next()) {
            list.add(resultSet.getString(1));
        }
        System.out.println(list);
        return list;
    }

    /**
     * 检查表中是否存在数据，存在返回 false
     *
     * @param statement
     * @return
     */
    public Boolean checkCatalogData(PreparedStatement statement) throws SQLException {
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            int count = resultSet.getInt("count");
            if (count == 0) {
                return TRUE;
            }
        }
        return FALSE;
    }

    /**
     * 删除表
     *
     * @param statement
     */
    public void deleteCatalog(PreparedStatement statement) throws SQLException {
        statement.execute();
    }

    public String partitionByDay(String catalog) {
        StringBuilder builder = new StringBuilder();
        LocalDate localDate = LocalDate.now();
        String date = localDate.plusDays(2).format(DateTimeFormatter.ISO_LOCAL_DATE);
        String ymm = localDate.plusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        builder.append("ALTER TABLE ").append(catalog).append(" ADD PARTITION (PARTITION ").append("p_day_").append(catalog).append("_").append(ymm)
                .append(" VALUES LESS THAN (TO_DAYS('").append(date).append("')").append("));");
        return builder.toString();
    }

    public String partitionByWeek(String catalog) {
        StringBuilder builder = new StringBuilder();
        LocalDate localDate = LocalDate.now();
        String date = localDate.plusWeeks(2).format(DateTimeFormatter.ISO_LOCAL_DATE);
        String ymm = localDate.plusWeeks(1).format(new DateTimeFormatterBuilder().parseCaseInsensitive()
                .appendValue(IsoFields.WEEK_BASED_YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
                .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 2)
                .optionalStart()
                .appendOffsetId().toFormatter());
        builder.append("ALTER TABLE ").append(catalog).append(" ADD PARTITION (PARTITION ").append("p_week_").append(catalog).append("_").append(ymm)
                .append(" VALUES LESS THAN (YEARWEEK('").append(date).append("')").append("));");
        return builder.toString();
    }

    public String partitionByMonth(String catalog) {
        StringBuilder builder = new StringBuilder();
        LocalDate localDate = LocalDate.now();
        String date = localDate.plusMonths(2).withDayOfMonth(1).format(DateTimeFormatter.ISO_LOCAL_DATE);
        String ymm = localDate.plusMonths(1).withDayOfMonth(1).format(DateTimeFormatter.ofPattern("yyyyMM"));
        builder.append("ALTER TABLE ").append(catalog).append(" ADD PARTITION (PARTITION ").append("p_month_").append(catalog).append("_").append(ymm)
                .append(" VALUES LESS THAN (TO_SECONDS('").append(date).append("')").append("));");
        return builder.toString();
    }

    public void delete(String sql) {
        try {
            connection.setAutoCommit(false);
            connection.prepareStatement(sql).execute();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
                logger.info("JDBC Transaction rolled back successfully");
            } catch (SQLException e1) {
                logger.error("SQLException in rollback", e1);
                e1.printStackTrace();
            }
            e.printStackTrace();
        }
    }

}