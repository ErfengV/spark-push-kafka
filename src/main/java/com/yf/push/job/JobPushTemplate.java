package com.yf.push.job;

import com.yf.push.config.BaseConfig;
import com.yf.push.config.KafkaConfig;
import com.yf.push.config.SparkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.SparkSession;

/**
 * @author erfeng
 * @version 1.0
 * @date 2024/5/9 16:41
 * @description: JOB 推送模板
 */
@Slf4j
public abstract  class JobPushTemplate {

    /**
     * spark 会话
     */
    protected SparkSession sparkSession;

    /**
     * kafka 生产者
     */
    protected Producer<String, String> producer;

    /**
     * topic 信息
     */
    protected String topic;

    /**
     *  日期信息
     */
    protected String date;


    protected void init(String ...args) throws Exception {
        if(args.length < 4){
            throw new Exception("参数范围错误，current length:" + args.length + " need length：4");
        }

        String mode     = args[0];
        String appName  = args[1];
        String date     = args[2];
        String topic    = args[3];
        log.info("当前执行模式：{}，应用名称：{}，数据日期：{}，topic：{}",mode,appName,date,topic);

        if(BaseConfig.ENV_PRD.equalsIgnoreCase(mode)){
            String userInfo = args[4];
            initPrd(appName, date,topic,userInfo);
        }else if(BaseConfig.ENV_DEV.equalsIgnoreCase(mode)){
            initDev(appName,date,topic);
        }else {
            initLocal(date,topic);
        }


    }

    protected void initDev(String appName,String date,String topic){
        this.sparkSession = SparkConfig.getSparkSession(appName);
        this.producer = new KafkaProducer<>(KafkaConfig.getDevConfig());
        this.date = date;
        this.topic = topic;
    }

    protected void initPrd(String appName,String date,String topic,String userInfo){
        this.sparkSession = SparkConfig.getSparkSession(appName);
        this.producer = new KafkaProducer<>(KafkaConfig.getPrdConfig(userInfo));
        this.date = date;
        this.topic = topic;
    }

    protected void initLocal(String date,String topic){
        this.date = date;
        this.topic = topic;
        this.producer = new KafkaProducer<>(KafkaConfig.getDevConfig());
    }

    /**
     * 编写推送kafka的程序
     */
    protected abstract void push();

    /**
     * 简易数据发送器
     * @param partition 分区信息
     * @param key
     * @param json
     */
    protected void send(int partition ,String key,String json){
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition,key, json);
        producer.send(record);
    }
    /**
     * 简易数据发送器
     * @param partition 分区信息
     * @param json
     */
    protected void send(int partition ,String json){
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition,"", json);
        producer.send(record);
    }

    /**
     * 自动关闭资源
     */
    public void finish(){
        if(this.producer !=null) {
            this.producer.close();
        }
        if(this.sparkSession !=null) {
            this.sparkSession.close();
        }
    }
    /**
     * 数组式参数运行
     * 参数顺序依次如下： 运行环境 ， 运行app名称 ，数据日期 ，topic ，如果是生产环境还需要输入userinfo信息
     * @param args
     */
    public final void run(String ...args) throws Exception {

        //初始化
        init(args);

        //推送
        push();

        //结束
        finish();
    }


}
