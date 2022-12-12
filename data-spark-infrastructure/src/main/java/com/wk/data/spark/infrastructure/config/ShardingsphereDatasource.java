//package com.wk.data.spark.infrastructure.config;
//
//import com.zaxxer.hikari.HikariDataSource;
//import org.apache.shardingsphere.driver.api.ShardingSphereDataSourceFactory;
//import org.apache.shardingsphere.infra.config.algorithm.ShardingSphereAlgorithmConfiguration;
//import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
//import org.apache.shardingsphere.sharding.api.config.rule.ShardingAutoTableRuleConfiguration;
//import org.apache.shardingsphere.sharding.api.config.strategy.sharding.StandardShardingStrategyConfiguration;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.stereotype.Component;
//
//import javax.sql.DataSource;
//import java.sql.SQLException;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.LinkedHashMap;
//import java.util.Map;
//import java.util.Properties;
//
///**
// * @Author: smash_hq
// * @Date: 2022/5/23 17:11
// * @Description: shardingsphere分库分表
// * @Version v1.0
// */
//@Component
//public class ShardingsphereDatasource {
//
//    private static Logger logger = LoggerFactory.getLogger(ShardingsphereDatasource.class);
//
//    @Value("${shardingsphere.datasource.common.driver-class-name}")
//    private String driver = "com.mysql.cj.jdbc.Driver";
//    @Value("${shardingsphere.datasource.common.sharding-count}")
//    private String mod = "4";
//    @Value("${shardingsphere.datasource.names}")
//    private String names = "ds0,ds1";
//    @Value("${shardingsphere.datasource.ds0.jdbc-url}")
//    private String url1 = "jdbc:mysql://192.168.238.129:3307/gmall_ums?serverTimezone=UTC&useSSL=false&useUnicode=true&characterEncoding=UTF-8";
//    @Value("${shardingsphere.datasource.ds0.username}")
//    private String username1 = "root";
//    @Value("${shardingsphere.datasource.ds0.password}")
//    private String password1 = "root";
//    @Value("${shardingsphere.datasource.ds1.jdbc-url}")
//    private String url2 = "jdbc:mysql://192.168.238.129:3316/gmall_ums?serverTimezone=UTC&useSSL=false&useUnicode=true&characterEncoding=UTF-8";
//    @Value("${shardingsphere.datasource.ds1.username}")
//    private String username2 = "root";
//    @Value("${shardingsphere.datasource.ds1.password}")
//    private String password2 = "root";
//
//    public DataSource dataSource(String code, String col) {
//        Properties properties = new Properties();
//        properties.setProperty("sql-show", "true");
//        try {
//            return ShardingSphereDataSourceFactory.createDataSource(dataSourceMap(), Collections.singleton(tableRules(code, col)), properties);
//        } catch (SQLException e) {
//            return null;
//        }
//    }
//
//    public Map<String, DataSource> dataSourceMap() {
//        Map<String, DataSource> dataSourceMap = new HashMap<>();
//        HikariDataSource dataSource1 = new HikariDataSource();
//        dataSource1.setJdbcUrl(url1);
//        dataSource1.setDriverClassName(driver);
//        dataSource1.setUsername(username1);
//        dataSource1.setPassword(password1);
//        dataSourceMap.put("ds0", dataSource1);
//
//        HikariDataSource dataSource2 = new HikariDataSource();
//        dataSource2.setJdbcUrl(url2);
//        dataSource2.setDriverClassName(driver);
//        dataSource2.setUsername(username2);
//        dataSource2.setPassword(password2);
//        dataSourceMap.put("ds1", dataSource2);
//
//        return dataSourceMap;
//    }
//
//    public ShardingRuleConfiguration tableRules(String code, String col) {
//        ShardingRuleConfiguration configuration = new ShardingRuleConfiguration();
//        ShardingAutoTableRuleConfiguration rules = getShardingAutoTableRuleConfiguration(code, col);
//        configuration.setAutoTables(Collections.singleton(rules));
//        Map<String, ShardingSphereAlgorithmConfiguration> shardingAlgorithms = new LinkedHashMap<>();
//        Properties shardingAlgorithmsProps = new Properties();
//        shardingAlgorithmsProps.setProperty("sharding-count", mod);
//        shardingAlgorithms.put("mod", new ShardingSphereAlgorithmConfiguration("MOD", shardingAlgorithmsProps));
//        configuration.setShardingAlgorithms(shardingAlgorithms);
//        Map<String, ShardingSphereAlgorithmConfiguration> keyGenerators = new LinkedHashMap<>();
//        Properties snows = new Properties();
//        snows.setProperty("worker-id", "123");
//        keyGenerators.put("snowflake", new ShardingSphereAlgorithmConfiguration("SNOWFLAKE", snows));
//        configuration.setKeyGenerators(keyGenerators);
//        return configuration;
//    }
//
//    public ShardingAutoTableRuleConfiguration getShardingAutoTableRuleConfiguration(String code, String col) {
//        ShardingAutoTableRuleConfiguration rules = new ShardingAutoTableRuleConfiguration(code, names);
//        rules.setShardingStrategy(new StandardShardingStrategyConfiguration(col, "mod"));
//        return rules;
//    }
//
//}
