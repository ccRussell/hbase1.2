package com.russell.bigdata.hbase.util;

import com.russell.bigdata.hbase.common.RowKeyDo;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author liumenghao
 * @Date 2019/3/5
 */
@Slf4j
public class HbaseClient {

    private volatile static HbaseClient instance;

    private Admin admin;

    /**
     * Hbase连接
     */
    private Connection connection;

    private HbaseClient(Connection conn, Admin ad) {
        this.connection = conn;
        this.admin = ad;
    }

    /**
     * 创建表
     *
     * @param tableName 表名
     * @param cols      需要创建的family
     * @throws IOException
     */
    public void createTable(String tableName, String[] cols) throws IOException {
        TableName table = TableName.valueOf(tableName);

        if (admin.tableExists(table)) {
            System.out.println("talbe is exists!");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
    }

    /**
     * 删除指定的表
     *
     * @param tableName 表名
     * @throws IOException
     */
    public void deleteTable(String tableName) throws IOException {
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
    }

    /**
     * 批量插入
     *
     * @param tableName    表名
     * @param rowKeyDoList 插入的内容
     */
    public void putData(String tableName, List<RowKeyDo> rowKeyDoList) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Put> puts = new ArrayList<>();
        for (RowKeyDo rowKeyDo : rowKeyDoList) {
            Put put = new Put(Bytes.toBytes(rowKeyDo.getRowKey()));
            put.addColumn(Bytes.toBytes(rowKeyDo.getColFamily()), Bytes.toBytes(rowKeyDo.getQualifier()),
                    Bytes.toBytes(rowKeyDo.getValue()));
            puts.add(put);
        }
        table.put(puts);
    }

    /**
     * 根据单个rowkey获取数据
     *
     * @param tableName
     * @param rowKeyDo
     * @return
     * @throws IOException
     */
    public Result getData(String tableName, RowKeyDo rowKeyDo) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKeyDo.getRowKey()));
        String colFamily = rowKeyDo.getColFamily();
        String qualifier = rowKeyDo.getQualifier();
        if (colFamily == null) {
            return table.get(get);
        }
        if (qualifier != null) {
            get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(qualifier));
        } else {
            get.addFamily(Bytes.toBytes(colFamily));
        }
        return table.get(get);
    }

    /**
     * 批量查询数据
     *
     * @param tableName
     * @param startRow
     * @param stopRow
     */
    public ResultScanner scanData(String tableName, String startRow, String stopRow) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner results = table.getScanner(scan);
        return results;
    }

    /**
     * 测试使用
     *
     * @param result
     */
    public void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timetamp:" + cell.getTimestamp() + " ");
            System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
        }
    }


    public static HbaseClient getInstance() {
        if (instance == null) {
            synchronized (HbaseClient.class) {
                if (instance == null) {
                    try {
                        Configuration conf = new Configuration();
                        conf.addResource("hbase-site.xml");
                        Configuration hbaseConfig = HBaseConfiguration.create(conf);
                        Connection connection = ConnectionFactory.createConnection(hbaseConfig);
                        Admin admin = connection.getAdmin();
                        instance = new HbaseClient(connection, admin);
                    } catch (Exception e) {
                        log.error("获取hbase 实例失败", e);
                    }
                }
            }
        }
        return instance;
    }


}
