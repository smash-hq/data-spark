package com.wk.data.spark.infrastructure.config;

import lombok.*;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @Author: smash_hq
 * @Date: 2021/8/23 15:54
 * @Description:
 * @Version v1.0
 */

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Configuration
@ConfigurationProperties(prefix = "spring.data.mongodb")
public class MongoProperties {
    private MongoProperty dataetl = new MongoProperty();

    private MongoProperty detection = new MongoProperty();
}
