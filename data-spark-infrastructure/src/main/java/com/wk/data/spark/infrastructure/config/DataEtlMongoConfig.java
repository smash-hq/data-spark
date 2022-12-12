//package com.wk.data.spark.infrastructure.config;
//
//import com.mongodb.ConnectionString;
//import com.mongodb.MongoClientSettings;
//import com.mongodb.MongoCredential;
//import com.mongodb.ServerAddress;
//import com.mongodb.client.MongoClient;
//import com.mongodb.client.MongoClients;
//import com.mongodb.connection.ConnectionPoolSettings;
//import org.springframework.beans.BeanUtils;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
//import org.springframework.boot.autoconfigure.domain.EntityScanner;
//import org.springframework.boot.context.properties.PropertyMapper;
//import org.springframework.context.ApplicationContext;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.context.annotation.Primary;
//import org.springframework.data.annotation.Persistent;
//import org.springframework.data.mapping.model.FieldNamingStrategy;
//import org.springframework.data.mongodb.MongoDatabaseFactory;
//import org.springframework.data.mongodb.core.MongoTemplate;
//import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
//import org.springframework.data.mongodb.core.convert.*;
//import org.springframework.data.mongodb.core.mapping.Document;
//import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
//
//import java.util.Collections;
//import java.util.concurrent.TimeUnit;
//
//import static java.util.Collections.singletonList;
//
///**
// * @Author: smash_hq
// * @Date: 2021/8/5 16:07
// * @Description: data-etl
// * @Version v1.0
// */
//@Configuration
//public class DataEtlMongoConfig {
//    private MongoProperties mongoProperties;
//    private MongoProperty mongoProperty;
//
//    @Autowired
//    public DataEtlMongoConfig(MongoProperties mongoProperties) {
//        this.mongoProperties = mongoProperties;
//        this.mongoProperty = mongoProperties.getDataetl();
//    }
//
//    @Autowired
//    private MongoMappingContext mongoMappingContext;
//
//    @Primary
//    @Bean(name = "dataetlMongoTemplate")
//    public MongoTemplate mongoTemplate() {
//        MongoDatabaseFactory mongoDatabaseFactory = mongoDbFactory();
//        MappingMongoConverter mappingMongoConverter = mappingMongoConverter(mongoMappingContext);
//        return new MongoTemplate(mongoDatabaseFactory, mappingMongoConverter);
//    }
//
//    public MappingMongoConverter mappingMongoConverter(MongoMappingContext context) {
//        DbRefResolver dbRefResolver = new DefaultDbRefResolver(mongoDbFactory());
//        MappingMongoConverter mappingConverter = new MappingMongoConverter(dbRefResolver, context);
//        mappingConverter.setCustomConversions(new MongoCustomConversions(Collections.emptyList()));
//        // Don't save _class to mongo
//        mappingConverter.setTypeMapper(new DefaultMongoTypeMapper(null));
//        return mappingConverter;
//    }
//
//    @Bean
//    @ConditionalOnMissingBean
//    public MongoMappingContext mongoMappingContext(ApplicationContext applicationContext) throws ClassNotFoundException {
//        PropertyMapper mapper = PropertyMapper.get().alwaysApplyingWhenNonNull();
//        MongoMappingContext context = new MongoMappingContext();
//        mapper.from(mongoProperty.isAutoIndexCreation()).to(context::setAutoIndexCreation);
//        context.setInitialEntitySet(new EntityScanner(applicationContext).scan(Document.class, Persistent.class));
//        Class<?> strategyClass = mongoProperty.getFieldNamingStrategy();
//        if (strategyClass != null) {
//            context.setFieldNamingStrategy((FieldNamingStrategy) BeanUtils.instantiateClass(strategyClass));
//        }
//        MongoCustomConversions conversions = new MongoCustomConversions(Collections.emptyList());
//        context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
//        return context;
//    }
//
//    public MongoDatabaseFactory mongoDbFactory() {
//        MongoClientSettings settings = computeClientSettings();
//        // 创建连接
//        MongoClient mongoClient = MongoClients.create(settings);
//        // 创建MongoDbFactory
//        return new SimpleMongoClientDatabaseFactory(mongoClient, mongoProperty.getDatabase());
//    }
//
//
//    /**
//     * 连接参数设置
//     *
//     * @return
//     */
//    private MongoClientSettings computeClientSettings() {
//        MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();
//        applyHostAndPort(settingsBuilder);
//        applyCredentials(settingsBuilder);
//        applyReplicaSet(settingsBuilder);
//        applyToConnectionPool(settingsBuilder);
//        return settingsBuilder.build();
//    }
//
//    /**
//     * 设置连接池
//     *
//     * @param builder
//     */
//    private void applyToConnectionPool(MongoClientSettings.Builder builder) {
//        ConnectionPoolSettings poolSetting = ConnectionPoolSettings.builder()
//                .maxWaitTime(mongoProperty.getMaxWaitTime(), TimeUnit.SECONDS)
//                .minSize(mongoProperty.getMinSize())
//                .maxSize(mongoProperty.getMaxSize())
//                .maxConnectionIdleTime(mongoProperty.getMaxConnectionIdleTime(), TimeUnit.SECONDS)
//                .maintenanceInitialDelay(mongoProperty.getMaintenanceInitialDelay(), TimeUnit.SECONDS)
//                .maxConnectionLifeTime(mongoProperty.getMaxConnectionLifeTime(), TimeUnit.SECONDS)
//                .build();
//        builder.applyToConnectionPoolSettings(poolBuilder -> poolBuilder.applySettings(poolSetting));
//    }
//
//    /**
//     * 配置副本信息
//     *
//     * @param builder
//     */
//    private void applyReplicaSet(MongoClientSettings.Builder builder) {
//        if (hasReplicaSet(mongoProperty)) {
//            builder.applyToClusterSettings((cluster) -> cluster.requiredReplicaSetName(mongoProperty.getReplicaSetName()));
//        }
//    }
//
//    /**
//     * 配置账户密码
//     *
//     * @param builder
//     */
//    private void applyCredentials(MongoClientSettings.Builder builder) {
//        if (hasCustomCredentials(mongoProperty)) {
//            String database = (mongoProperty.getAuthenticationDatabase() != null)
//                    ? mongoProperty.getAuthenticationDatabase() : mongoProperty.getMongoClientDatabase();
//
//            builder.credential((MongoCredential.createCredential(mongoProperty.getUsername(), database, mongoProperty.getPassword().toCharArray())));
//        }
//    }
//
//    /**
//     * 配置连接方式
//     *
//     * @param settings
//     */
//    private void applyHostAndPort(MongoClientSettings.Builder settings) {
//        if (hasCustomAddress(mongoProperty)) {
//            settings.applyToClusterSettings((cluster) -> cluster.hosts(singletonList(new ServerAddress(mongoProperty.getHost(), mongoProperty.getPort()))));
//            return;
//        }
//        settings.applyConnectionString(new ConnectionString(mongoProperty.determineUri()));
//    }
//
//    private boolean hasCustomAddress(MongoProperty properties) {
//        return properties.getHost() != null || properties.getPort() != null;
//    }
//
//    private boolean hasCustomCredentials(MongoProperty properties) {
//        return properties.getUsername() != null && properties.getPassword() != null;
//    }
//
//    private boolean hasReplicaSet(MongoProperty properties) {
//        return properties.getReplicaSetName() != null;
//    }
//
//
//}
