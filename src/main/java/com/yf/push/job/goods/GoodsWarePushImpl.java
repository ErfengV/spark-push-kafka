package com.yf.push.job.goods;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.yf.push.config.BaseConfig;
import com.yf.push.job.JobPushTemplate;
import com.yf.push.job.goods.bean.GoodsReCalcAutoStkResult;
import com.yf.push.util.RowUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;


import java.util.*;

/**
 * @author erfeng
 * @version 1.0
 * @date 2024/5/9 16:52
 * @description: 一个推送任务 推送一个topic数据
 */
@Slf4j
public class GoodsWarePushImpl extends JobPushTemplate {


    @Override
    public void push() {


        Dataset<GoodsReCalcAutoStkResult> transfer =
                RowUtils.rowToObj(sparkSession.sql(
                          " select  phmc_code as storeCode, ware_code as supplyWarehouseCode, " +
                                 " bill_code as billCode, 'TRANSFER' as dataStatus,bill_json as  changeData,stat_date as statDate " +
                                 " from prd_tmp.at_phmc_goods_re_calc_auto_stk_result_d  " +
                                 " where stat_date = '" + date + "'")
                        , GoodsReCalcAutoStkResult.class);

        Map<String, Iterable<GoodsReCalcAutoStkResult>> transferMap =
                transfer.toJavaRDD().groupBy(GoodsReCalcAutoStkResult::getSupplyWarehouseCode).collectAsMap();

        Object[] wareCodes = transferMap.keySet().toArray();

        for (int i = 0; i < wareCodes.length; i++) {
            //获取分区 和 仓库编码
            int partition = i % 3;
            String wareCode = (String) wareCodes[i];
            log.info("{} is transferring，kafka partition：{}",wareCode,partition);

            //推送数据
            process(wareCode,"START",partition);
            transfer(transferMap.get(wareCode), partition);
            process(wareCode,"END",partition );

        }

    }

    protected void process(String wareCode , String status , int partition){
        // 构建json数据
        String json = JSONUtil.createObj()
                .set("supplyWarehouseCode", wareCode)
                .set("dataStatus", status)
                .set("statDate", date).toString();
        send(partition,BaseConfig.sdf.format(System.currentTimeMillis()),json);
    }

    private void transfer(Iterable<GoodsReCalcAutoStkResult> goodsReCalcAutoStkResult , int partition){
        for (GoodsReCalcAutoStkResult reCalcAutoStkResult : goodsReCalcAutoStkResult) {
            JSONArray changeData = JSONUtil.parseArray(reCalcAutoStkResult.getChangeData());

            JSONObject json = JSONUtil.parseObj(reCalcAutoStkResult);
            json.remove("changeData");
            json.set("changeData", changeData);
            send(partition,BaseConfig.sdf.format(System.currentTimeMillis()),json.toString());
        }
    }


}
