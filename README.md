# spark推送kafka的模板工具

## 简介

主要是方便以后遇到数仓向kafka推送数据的项目可以快速响应，特意做了一个模板工具

## 目录

- [简介](#简介)
- [目录](#目录)
- [功能](#功能)
- [使用方法](#使用方法)
- [代码结构](#代码结构)


## 功能

- 利用spark将数仓的数据推送到kafka里面
- 同时做了一个RowUtils小工具，方便row对象直接转换成java bean对象

## 使用方法

1. 克隆或下载本项目代码：
   ```bash
   git clone https://github.com/Erfeng_V/spark-push-kafka.git

2. 继承成JobPushTemplate 类，并实现push方法


## 代码结构

- `KafkaConfig.java`：kafka配置，分开发配置和生产配置。
- `SparkConfig.java`：spark配置。
- `JobPushTemplate.java`：任务推送模板。
- `RowUtils.java`：行对象转java bean对象工具。


