package com.wk.data.spark.infrastructure.config;

import com.mongodb.ConnectionString;
import lombok.*;

/**
 * @Author: smash_hq
 * @Date: 2021/8/23 15:45
 * @Description: mongo数据源信息
 * @Version v1.0
 */

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class MongoProperty {
    /**
     * Default port used when the configured port is {@code null}.
     */
    public static final int DEFAULT_PORT = 27017;

    /**
     * Default URI used when the configured URI is {@code null}.
     */
    public static final String DEFAULT_URI = "mongodb://localhost/test";

    /**
     * Mongo server host. Cannot be set with URI.
     */
    private String host;

    /**
     * Fully qualified name of the FieldNamingStrategy to use.
     */
    private Class<?> fieldNamingStrategy;

    /**
     * Mongo server port. Cannot be set with URI.
     */
    private Integer port = null;

    /**
     * Mongo database URI. Cannot be set with host, port, credentials and replica set
     * name.
     */
    private String uri;

    /**
     * Database name.
     */
    private String database;

    /**
     * Authentication database name.
     */
    private String authenticationDatabase;

    /**
     * GridFS database name.
     */
    private String gridFsDatabase;

    /**
     * Login user of the mongo server. Cannot be set with URI.
     */
    private String username;

    /**
     * Login password of the mongo server. Cannot be set with URI.
     */
    private String password;

    /**
     * Required replica set name for the cluster. Cannot be set with URI.
     */
    private String replicaSetName;

    /**
     * Whether to enable auto-index creation.
     */
    private Boolean autoIndexCreation;

    private long maxWaitTime = 1000 * 60 * 2L;
    private int maxSize = 100;
    private int minSize = 0;
    private long maxConnectionLifeTime;
    private long maxConnectionIdleTime;
    private long maintenanceInitialDelay;
    private long maintenanceFrequency;

    public String determineUri() {
        return (this.uri != null) ? this.uri : DEFAULT_URI;
    }

    public String getMongoClientDatabase() {
        if (this.database != null) {
            return this.database;
        }
        return new ConnectionString(determineUri()).getDatabase();
    }

    public Class<?> getFieldNamingStrategy() {
        return this.fieldNamingStrategy;
    }

    public Boolean isAutoIndexCreation() {
        return this.autoIndexCreation;
    }
}
