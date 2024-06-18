package com.yf.push.config;

import java.util.Properties;

/**
 * @author erfeng
 * @version 1.0
 * @date 2024/5/9 16:26
 * @description: kafka配置信息
 */
public class KafkaConfig {

    public static Properties getDevConfig(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "your kafka servers");
        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=your kafka name password=your kafka pwd;");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }

    public static Properties getPrdConfig(String userInfo){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "your kafka servers");
        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("sasl.jaas.config", userInfo);
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }
}
