package com.chiangshin.hbase;

import java.io.IOException;
import java.util.Iterator;
import junit.runner.BaseTestRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/**
 * @Author jx
 * @Date 2019/9/16 23:07
 */
public class HbaseDemo {
    private static Configuration config=null;
    private static HBaseAdmin admin=null;
    //zookeeper集群地址
    private final static String   HB_ZK_QUORUM="hadoop102,hadoop103,hadoop104";
    //zk端口号
    private final static String HB_ZK_PORT="2181";
    private static HTablePool tp = null;

    static {
        try {
            config= HBaseConfiguration.create();
            config.set("hbase.zookeeper.property.clientPort", HB_ZK_PORT);
            config.set("hbase.zookeeper.quorum",HB_ZK_QUORUM);
            admin = new HBaseAdmin(config);
            tp = new HTablePool(config, 10);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 表是否存在
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException {
        return admin.tableExists(tableName);
    }

    /**
     * 创建表
     * @param tableName
     */
    public static void createTable(String tableName,String... columnFamily){
        TableName table  = TableName.valueOf(tableName);
        try {
            if (!isTableExist(tableName)){
                System.out.println(tableName + "table is not exist");
                // 创建表属性对象，表名需要转字节
                HTableDescriptor descriptor = new HTableDescriptor(table);

                // 创建多个列族
                for (String cf: columnFamily){
                    descriptor.addFamily(new HColumnDescriptor(cf));
                }
                // 根据对表的配置，创建表
                admin.createTable(descriptor);
                System.out.println("创建表"+tableName+"成功！");

            }else{
                System.out.println(tableName + "table is exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 删除表，删除前需要disable,与disable 相反的是enable
     * @param tableName
     */
    public static void dropTable(String tableName){
        try {
            if(!isTableExist(tableName)){
                System.out.println("表"+tableName+"不存在");
            }else{
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println("删除表"+tableName+"成功！");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入数据
     * @param tableName
     * @param rowKey
     * @param columFamily
     * @param colum
     * @param value
     * @throws IOException
     */
    public static void addRowData(String tableName,String rowKey, String columFamily,String colum,String value)
            throws IOException {
        // 创建Htable对象
        HTable hTable = new HTable(config,tableName);

        Put put = new Put(Bytes.toBytes(rowKey));

        put.add(Bytes.toBytes(columFamily),Bytes.toBytes(colum),Bytes.toBytes(value));

        hTable.put(put);
        hTable.close();
    }
    @Test
    public  void addDateTest() throws Exception{
        createTable("person","age","job");
        addRowData("person","1001","age","year","1985");
        addRowData("person","1001","age","month","12");
        addRowData("person","1001","age","day","01");
        addRowData("person","1001","job","dept","软件开发部");
        addRowData("person","1001","job","level","P9");
        IOUtils.closeStream(admin);
    }

    public static void deleteMultiRow(String tableName,String... rows){
        try {
            HTable hTable = new HTable(config,tableName);
            for (String row : rows) {
                Delete delete = new Delete(Bytes.toBytes(row));
                hTable.delete(delete);
            }
            hTable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteMultiRowTest() {
        deleteMultiRow("person","1001");
    }

    /**
     * 得到所有数据
     * @param tableName
     * @throws IOException
     */
    public void getAllRows(String tableName) throws IOException {
        HTable hTable = new HTable(config,tableName);
        Scan scan = new Scan();
        ResultScanner scanner = hTable.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            Result res = iterator.next();
            byte[] value = res.getValue("age".getBytes(), "year".getBytes());
            System.out.println(new String(value));
        }
        hTable.close();

    }

    @Test
    public void getAllRowsTest() throws Exception{
        getAllRows("person");
    }

}
