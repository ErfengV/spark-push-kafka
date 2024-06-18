package com.yf.push.job.goods.bean;

import lombok.Data;

/**
 * @author erfeng
 * @date 2024/5/10 10:16
 * @description: 仓库数据
 */
@Data
public class GoodsReCalcAutoStkResult {


    private String storeCode;
    private String supplyWarehouseCode;
    private String billCode;
    private String dataStatus;
    private String changeData;
    private String statDate;
}
