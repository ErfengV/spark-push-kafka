package com.yf.push;

import com.yf.push.config.BaseConfig;
import com.yf.push.job.JobPushTemplate;
import com.yf.push.job.test.DataPushImpl;

/**
 * @author erfeng
 * @version 1.0
 * @date 2024/6/17 19:06
 * @description: 本地推送是本地自己造数据向kafka推送
 */
public class Test {

    public static void main(String[] args) throws Exception {

        JobPushTemplate pushTemplate = new DataPushImpl();
        //根据实际情况选择本地 (LOCAL)，测试(DEV)，以及生产(PRD)环境
        pushTemplate.run(BaseConfig.ENV_LOCAL, "SPARK_PUSH_KAFKA", "20240615"
                    , "yfom_budget_zb_change_task");


    }
}
