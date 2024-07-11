package com.zoo.hbase.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsClientTest {
    Configuration configuration;
    FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        configuration= new Configuration();
        fs  = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "hadoop");
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    /**
     * 测试创建目录
     */
    @Test
    public void testMkDirs() throws URISyntaxException, IOException, InterruptedException {
        fs.mkdirs(new Path("/hadoop/java"));
    }

    /**
     * 测试列出文件夹和文件
     */
    @Test
    public void testList() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            if (fileStatus.isFile()) {
                System.out.println("file: "+fileStatus.getPath().getName());
            }else {
                System.out.println("dir: "+fileStatus.getPath().getName());
            }
        }
    }

    /**
     * 测试列出文件描述信息,HDFS 文件详情查看
     */
    @Test
    public void testListFile() throws IOException {
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/"), false);

        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus fileStatus = locatedFileStatusRemoteIterator.next();

            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    /**
     * 测试文件上传
     */
    @Test
    public void fileUpload() throws IOException {
        fs.mkdirs(new Path("/test"));
        fs.copyFromLocalFile(new Path("Y:\\HDFSClient.zip"), new Path("/test"));
    }
    /**
     * 测试文件下载
     */
    @Test
    public void fileDownload() throws IOException {
        fs.copyToLocalFile(new Path("/test/HDFSClient.zip"), new Path("Y:\\"));
    }

    /**
     * 测试文件更名和移动
     */
    @Test
    public void fileRename() throws IOException {
        fs.rename(new Path("/test/HDFSClient.zip"), new Path("/test/HDFSClient.zip.test"));
    }

    /**
     * 测试删除文件和目录
     */
    @Test
    public void objectDelete() throws IOException {
        fs.delete(new Path("/test/HDFSClient.zip.test"), true);
    }
}
