package com.chiangshin.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/**
 * @Author jx
 * @Date 2019/8/9 1:46
 */
public class IOHDFS {

    /**
     * 文件上传
     */
    @Test
    public void putFIleToHDFS() throws IOException, URISyntaxException, InterruptedException {
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:8020"), configuration, "atguigu");

        //  获取输出流
        FSDataOutputStream fos = fs.create(new Path("/use/atguigu/output/caoliu.py"));

        // 获取输入流
        FileInputStream fis = new FileInputStream("D:\\git\\py2\\caoliu.py");
        try {
            IOUtils.copyBytes(fis, fos, configuration);
        }finally {
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
        }
    }

    /**
     * 文件下载 第一块
     */
    @Test
    public void download1() throws URISyntaxException, IOException, InterruptedException {
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:8020"),config,"atguigu");

        FSDataInputStream fis = fs.open(new Path("/user/atguigu/input/jdk-7u79-linux-x64.gz"));

        FileOutputStream fos = new FileOutputStream("d://test//jdk-7u79-linux-x64.gz.bak1");

        byte[] buf = new byte[1024];
        for (int i = 0; i < 128 * 1024; i++) {
            fis.read(buf);
            fos.write(buf);
        }

        // 6 关闭流
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);

    }

    /**
     * 文件下载 第二块
     */
    @Test
    public void download2() throws URISyntaxException, IOException, InterruptedException {
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:8020"),config,"atguigu");

        FSDataInputStream fis = fs.open(new Path("/user/atguigu/input/jdk-7u79-linux-x64.gz"));

        FileOutputStream fos = new FileOutputStream("d://test//jdk-7u79-linux-x64.gz.bak2");
        // 5 定位偏移量（第二块的首位）
        fis.seek(128*1024*1024);

        IOUtils.copyBytes(fis,fos,2048,true);

    }
}
