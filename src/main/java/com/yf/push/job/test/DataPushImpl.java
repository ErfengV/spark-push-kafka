package com.yf.push.job.test;


import com.yf.push.job.JobPushTemplate;
import lombok.extern.slf4j.Slf4j;


/**
 * @author erfeng
 * @version 1.0
 * @date 2024/5/9 16:52
 * @description: 一个推送任务 推送一个topic数据
 */
@Slf4j
public class DataPushImpl extends JobPushTemplate {


    @Override
    public void push() {

        String budgetChangeId = "A123";


        send(1,"[{\n" +
                "    \"budgetChangeId\":\"B123\",\n" +
                "    \"version\": \"版本，同 调整ID \",\n" +
                "    \"refBudgetChangeId\":\"123456\", \n" +
                "    \"changeType\":\"2\",\n" +
                "    \"phmcFlag\":\"0(直营)/1(加盟)\" ,\n" +
                "    \"phmcCode\":\"门店编码\",\n" +
                "    \"phmcName\":\"门店名称\",\n" +
                "    \"buisCode\": \"业务场景编码\",\n" +
                "    \"buisName\": \"业务场景名称\", \n" +
                "    \"channelType\": \"渠道类型\",\n" +
                "    \"syncGroupRankFlag\": \"M\",\n" +
                "    \"syncMngeRankFlag\": \"M\",\n" +
                "    \"syncSdAreaRankFlag\": \"M\",\n" +
                "    \"syncStrgRankFlag\": \"M\",\n" +
                "    \"syncDistRankFlag\": \"M\",\n" +
                "    \"syncPhmcRankFlag\": \"M\",\n" +
                "    \"yearMonth\": \"2024-06\",\n" +
                "\n" +
                "    \"desc\": \"原因及备注\",\n" +
                "    \"saleTask\": \"日均销售额任务\",\n" +
                "    \"grosProfTask\": \"日均毛利额任务\",\n" +
                "    \"dataTypeFlag\":\"data\",\n" +
                "    \"opManCode\":\"操作人\",\n" +
                "    \"opManName\":\"操作名称\",\n" +
                "    \"sendTime\": \"导入时间，仅做记录\"\n" +
                "}]");

        for (int i = 0; i < 3; i++) {
            send(i,"[{\t\t\n" +
                    "    \"changeType\":\"0\",\t\t\n" +
                    "    \"budgetChangeId\":\"B123\",\t\t\n" +
                    "    \"version\": \"123\",\t\t\n" +
                    "    \"endFlag\": \"true\",\t\t\n" +
                    "    \"sendCount\": \"1\",\t\t\n" +
                    "    \"dataTypeFlag\":\"flag\",\t\t\n" +
                    "    \"sendTime\": \"数据发送时间\"\t\t\n" +
                    "}]\t\t");
        }

    }



}
