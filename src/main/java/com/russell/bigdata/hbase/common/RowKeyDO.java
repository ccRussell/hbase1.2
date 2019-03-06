package com.russell.bigdata.hbase.common;

import lombok.Data;

/**
 * @author liumenghao
 * @Date 2019/3/5
 */
@Data
public class RowKeyDO {

    /**
     * 插入内容的rowKey
     */
    private String rowKey;

    /**
     * 列族
     */
    private String colFamily;

    /**
     * 列名
     */
    private String qualifier;

    /**
     * 插入的内容
     */
    private String value;

}
