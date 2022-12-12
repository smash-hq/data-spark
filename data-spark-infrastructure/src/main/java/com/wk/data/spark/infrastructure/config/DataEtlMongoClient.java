package com.wk.data.spark.infrastructure.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.connection.ConnectionPoolSettings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;

/**
 * @Author: smash_hq
 * @Date: 2021/9/23 16:43
 * @Description: client
 * @Version v1.0
 */
@Configuration
public class DataEtlMongoClient {
    private MongoProperties mongoProperties;
    private MongoProperty mongoProperty;

    @Autowired
    public DataEtlMongoClient(MongoProperties mongoProperties) {
        this.mongoProperties = mongoProperties;
        this.mongoProperty = mongoProperties.getDataetl();
    }

    @Primary
    @Bean(name = "dataEMongoClient")
    public MongoClient mongoDbFactory() {
        MongoClientSettings settings = computeClientSettings();
        // 创建连接
        return MongoClients.create(settings);
    }

    private MongoClientSettings computeClientSettings() {
        MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();
        applyHostAndPort(settingsBuilder);
        applyCredentials(settingsBuilder);
        applyReplicaSet(settingsBuilder);
        applyToConnectionPool(settingsBuilder);
        return settingsBuilder.build();
    }

    private void applyToConnectionPool(MongoClientSettings.Builder builder) {
        ConnectionPoolSettings poolSetting = ConnectionPoolSettings.builder()
                .maxWaitTime(mongoProperty.getMaxWaitTime(), TimeUnit.SECONDS)
                .minSize(mongoProperty.getMinSize())
                .maxSize(mongoProperty.getMaxSize())
                .maxConnectionIdleTime(mongoProperty.getMaxConnectionIdleTime(), TimeUnit.SECONDS)
                .maintenanceInitialDelay(mongoProperty.getMaintenanceInitialDelay(), TimeUnit.SECONDS)
                .maxConnectionLifeTime(mongoProperty.getMaxConnectionLifeTime(), TimeUnit.SECONDS)
                .build();
        builder.applyToConnectionPoolSettings(poolBuilder -> poolBuilder.applySettings(poolSetting));
    }

    /**
     * 配置副本信息
     *
     * @param builder
     */
    private void applyReplicaSet(MongoClientSettings.Builder builder) {
        if (hasReplicaSet(mongoProperty)) {
            builder.applyToClusterSettings(cluster -> cluster.requiredReplicaSetName(mongoProperty.getReplicaSetName()));
        }
    }

    /**
     * 配置账户密码
     *
     * @param builder
     */
    private void applyCredentials(MongoClientSettings.Builder builder) {
        if (hasCustomCredentials(mongoProperty)) {
            String database = (mongoProperty.getAuthenticationDatabase() != null)
                    ? mongoProperty.getAuthenticationDatabase() : mongoProperty.getMongoClientDatabase();

            builder.credential((MongoCredential.createCredential(mongoProperty.getUsername(), database, mongoProperty.getPassword().toCharArray())));
        }
    }

    /**
     * 配置连接方式
     *
     * @param settings
     */
    private void applyHostAndPort(MongoClientSettings.Builder settings) {
        if (hasCustomAddress(mongoProperty)) {
            settings.applyToClusterSettings(cluster -> cluster.hosts(singletonList(new ServerAddress(mongoProperty.getHost(), mongoProperty.getPort()))));
            return;
        }
        settings.applyConnectionString(new ConnectionString(mongoProperty.determineUri()));
    }

    private boolean hasCustomAddress(MongoProperty properties) {
        return properties.getHost() != null || properties.getPort() != null;
    }

    private boolean hasCustomCredentials(MongoProperty properties) {
        return properties.getUsername() != null && properties.getPassword() != null;
    }

    private boolean hasReplicaSet(MongoProperty properties) {
        return properties.getReplicaSetName() != null;
    }


}
