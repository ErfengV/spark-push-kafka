package com.yf.push.util;




import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 *
 * @author:erfeng_v
 * @create: 2022-12-03 17:58
 * @Description: row对象的工具
 *
 */
@Slf4j
public class RowUtils {

    /**
     * 循环赋值
     * @param source row对象
     * @param target 需要转换的目标对象
     * @return 返回目标对象
     *
     */
    public static <T> T setValue(Row source, T target){
        return setValue(source,target,null);
    }

    /**
     * 字段赋值
     * @param source row对象
     * @param target 需要转换的目标对象
     * @return 返回目标对象
     */
    public static <T> T setValue(Row source, T target,String group){
        try {
            Field[] fields = target.getClass().getDeclaredFields();
            for (Field field : fields) {
                String setMethodName = "set" + (field.getName().charAt(0) + "").toUpperCase() + field.getName().substring(1);
                Method setMethod = target.getClass().getMethod(setMethodName,field.getType());
                Object rowObj = null;
                try {
                    rowObj = source.getAs(field.getName());
                }catch (Exception e){
                    log.error("获取:{} 字段异常,请查询是否有无该字段",field.getName());
                }
                setMethod.invoke(target,rowObj);

            }
        }catch (ReflectiveOperationException e){

            e.printStackTrace();
        }
        return target;
    }
    /**
     * 行对象转换成目标对象
     * @param dataset
     * @param c
     * @return
     */
    public static <T> Dataset<T> rowToObj(Dataset<Row> dataset, Class<T> c){
        return dataset.map((MapFunction<Row, T>) row -> setValue(row, c.newInstance()), Encoders.bean(c));
    }
    /**
     * 行对象转换成目标对象
     * @param dataset
     * @param c
     * @param group
     * @return
     */
    public static <T> Dataset<T> rowToObj(Dataset<Row> dataset, Class<T> c,String group){
        return dataset.map((MapFunction<Row, T>) row -> setValue(row, c.newInstance(),group), Encoders.bean(c));
    }
    /**
     * 表直接转换成目标对象
     * @param sparkSession spark
     * @param tableName 表名
     * @param condition 条件值 例：where stat_date = '20221103'
     * @param c 目标值
     * @return 目标对象dataset
     */
    public static <T> Dataset<T> tableToObj(SparkSession sparkSession,String tableName,String condition,Class<T> c){
        Dataset<Row> dataset = sparkSession.read().table(tableName).where(condition);
        return rowToObj(dataset,c);
    }

    public static <T> Dataset<T> tableToObj(SparkSession sparkSession,String tableName,Class<T> c){
        Dataset<Row> dataset = sparkSession.read().table(tableName);
        return rowToObj(dataset,c);
    }



    /**
     * 表级别 辅助持久化的函数
     * @param sparkSession spark会话
     * @param table 表
     * @param view 视图名称
     * @param condition 条件 如：stat_date ='20230206'
     * @param level 存储级别
     * @throws AnalysisException
     */
    public static void persistTable(SparkSession sparkSession,String table
            ,String view,String condition,StorageLevel level) throws AnalysisException {
        Dataset<Row> rowDataset = sparkSession.read().table(table).
                where(condition);
        rowDataset.createTempView(view);
        sparkSession.table(view).persist(level);
        //打印出来数据的总和，action
        log.info("{} count:{}",view,rowDataset.count());
    }

}
