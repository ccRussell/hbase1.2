package com.russell.bigdata.hbase.example;

import com.russell.bigdata.hbase.common.RowKeyDO;
import com.russell.bigdata.hbase.util.HbaseClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.util.ArrayList;
import java.util.List;

/**
 * 测试接口使用
 *
 * @author liumenghao
 * @Date 2019/3/6
 */
@Slf4j
public class TestMain {

    private static String tableName = "hbase:user";
    private static String[] colFamily = {"userInfo"};

    public static void main(String[] args) throws Exception {
        HbaseClient hbaseClient = HbaseClient.getInstance();
        // 测试创建表
        System.out.println("创建表开始......");
        hbaseClient.createTable(tableName, colFamily);
        List<String> tables = hbaseClient.listTables();

        System.out.println("=======================================");
        System.out.println("插入单rowkey，多列的数据");
        hbaseClient.putData(tableName, generateSingleRows());
        RowKeyDO query = new RowKeyDO();
        query.setRowKey("hbase_rowkey_0");
        Result result = hbaseClient.getData(tableName, query);
        hbaseClient.showCell(result);

        System.out.println("=======================================");
        System.out.println("插入多rowkey，多列的数据");
        hbaseClient.putData(tableName, generateMuilteRows());
        System.out.println("批量查询数据......");
        String startRow = "hbase_rowkey_0";
        String stopRow = "hbase_rowkey_3";
        ResultScanner scanner = hbaseClient.scanData(tableName, startRow, stopRow);
        for (Result result1 : scanner) {
            hbaseClient.showCell(result1);
        }
        System.out.println("测试完成......");

    }


    public static List<RowKeyDO> generateSingleRows() {
        List<RowKeyDO> rows = new ArrayList<>();
        RowKeyDO rowKeyDO = new RowKeyDO();
        rowKeyDO.setRowKey("hbase_rowkey_0");
        rowKeyDO.setColFamily(colFamily[0]);
        rowKeyDO.setQualifier("userName");
        rowKeyDO.setValue("russell");
        rows.add(rowKeyDO);

        RowKeyDO rowKeyDO1 = new RowKeyDO();
        rowKeyDO1.setRowKey("hbase_rowkey_0");
        rowKeyDO1.setColFamily(colFamily[0]);
        rowKeyDO1.setQualifier("userAge");
        rowKeyDO1.setValue("25");
        rows.add(rowKeyDO1);

        return rows;
    }

    public static List<RowKeyDO> generateMuilteRows() {
        List<RowKeyDO> rows = new ArrayList<>();
        RowKeyDO rowKeyDO = new RowKeyDO();
        rowKeyDO.setRowKey("hbase_rowkey_1");
        rowKeyDO.setColFamily(colFamily[0]);
        rowKeyDO.setQualifier("userName");
        rowKeyDO.setValue("russell");
        rows.add(rowKeyDO);

        RowKeyDO rowKeyDO1 = new RowKeyDO();
        rowKeyDO1.setRowKey("hbase_rowkey_2");
        rowKeyDO1.setColFamily(colFamily[0]);
        rowKeyDO1.setQualifier("userName");
        rowKeyDO1.setValue("darview");
        rows.add(rowKeyDO1);

        return rows;
    }
}
