package com.chiangshin.hadoop.hdfs;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * 对hdfs各种操作
 * @Author jx
 * @Date 2019/7/25 22:49
 */
public class CRUDHDFS {

    public static void main(String[] args) throws IOException {
        Configuration config = new Configuration();
        config.set("fs.defaultFS","hdfs://hadoop100:8020");
        FileSystem fileSystem = FileSystem.get(config);
        // 创建文件
        fileSystem.create(new Path("/user/atguigu/xx.txt"));
        // 创建目录
        fileSystem.mkdirs(new Path("/user/minmin"));
        // 删除文件或目录
        boolean mkdirs = fileSystem.delete(new Path("/chiangshindd"),true);
        // copy 到本地
        fileSystem.copyToLocalFile(false,new Path("/jiangxinshi.txt"),new Path("d://jiangxin.txt"),true);
        // 从本地copy
        fileSystem.copyFromLocalFile(new Path("D:\\workspace\\app_staffhome_test.sql"),new Path("/user/minmin/app_staffhome_test.sql"));
       

        fileSystem.close();


    }

    @Test
    public void upload() throws IOException, URISyntaxException, InterruptedException {
        Configuration config = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop100:8020"),config,"atguigu");
        // 删除文件或目录
        boolean mkdirs = fileSystem.delete(new Path("/user/atguigu/xx.txt"),true);
        // 从本地copy
       // fileSystem.copyFromLocalFile(new Path("D:\\下载\\尚硅谷2018大数据全套（8月8更新版）\\02_尚硅谷大数据技术之Hadoop\\02_尚硅谷大数据技术之Hadoop\\1.笔记\\业务流程.png"),new Path("/user/minmin/业务流程.png"));
        fileSystem.close();
    }

}
