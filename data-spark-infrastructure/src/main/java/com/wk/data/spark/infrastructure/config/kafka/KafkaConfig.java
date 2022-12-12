package com.wk.data.spark.infrastructure.config.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

/**
 * @Author: gwl
 * @Description:
 * @Date: Created in 2020/7/30 13:24
 **/
@Configuration
@EnableConfigurationProperties({KafkaProperties.class})
@EnableKafka
@RefreshScope
@Slf4j
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * 生产者工厂配置
     *
     * @return
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(senderProps(kafkaProperties));
    }

    /**
     * kafka发送消息模板
     *
     * @return
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }


    private Map<String, Object> senderProps(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        return props;
    }
}
