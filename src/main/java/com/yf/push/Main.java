package com.yf.push;

import com.yf.push.config.BaseConfig;
import com.yf.push.job.JobPushTemplate;
import com.yf.push.job.goods.GoodsWarePushImpl;


/**
 * @author erfeng
 * @version 1.0
 * @date 2024/5/9 11:41
 * @description: 主程序入口
 *  依次填入参数信息： 运行环境 ， 运行app名称 ，数据日期 ，topic ，如果是生产环境还需要输入userinfo信息
 *  例如："PRD","PUSH_KAFKA","20240508","TOPIC","USERINFO"
 */
public class Main {
    public static void main(String[] args) throws Exception {

        JobPushTemplate pushTemplate = new GoodsWarePushImpl();
        //根据实际情况选择本地 (LOCAL)，测试(DEV)，以及生产(PRD)环境
        String[] param = args[0].split(",");

        if(param.length > 1) {
            pushTemplate.run(BaseConfig.ENV_DEV, "SPARK_PUSH_KAFKA", param[0]
                    ,param[1]);
        }else {
            pushTemplate.run(BaseConfig.ENV_DEV, "SPARK_PUSH_KAFKA", param[0]
                    , "at_phmc_goods_re_calc_auto_stk_result_d");
        }

    }


}
