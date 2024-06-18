package com.yf.push.config;

import org.apache.spark.sql.SparkSession;

/**
 * @author erfeng
 * @version 1.0
 * @date 2024/5/9 16:28
 * @description:
 */
public class SparkConfig {

    public static SparkSession getSparkSession(String appName) {
        return SparkSession.builder()
                .master("yarn")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryoserializer.buffer.max", "1g")
                .config("spark.sql.hive.convertMetastoreOrc", "false")
                .config("hive.metastore.dml.events", "false")
                .config("spark.yarn.priority", 5)
                .config("spark.rpc.askTimeout", "360s")
                .config("spark.sql.adaptive.skewJoin.enabled", true)
                .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "564MB")
                .config("spark.sql.shuffle.partitions", 800)
                .config("spark.default.parallelism", 500)
                .enableHiveSupport()
                .appName(appName)
                .getOrCreate();
    }
}
