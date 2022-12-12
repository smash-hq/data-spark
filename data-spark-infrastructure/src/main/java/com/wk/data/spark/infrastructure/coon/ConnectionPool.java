package com.wk.data.spark.infrastructure.coon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @program: data-spark-job
 * @description: 数据库连接工具
 * @author: gwl
 * @create: 2021-12-14 18:40
 **/
public class ConnectionPool {

    /**
     * 连接 关系型 数据库
     *
     * @param url
     * @param user
     * @param password
     * @return
     */
    public static Connection getConnection(String url, String user, String password, String driver) throws SQLException {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return DriverManager.getConnection(url, user, password);
    }

    public static void close(Connection connection) throws SQLException {
        connection.close();
    }


}
