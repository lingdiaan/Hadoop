package com.yj.hdfs;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsClient {
    private FileSystem fileSystem;
    @Before
    public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {
        //连接的nn地址
        URI uri = new URI("hdfs://hadoop102:8020");
        //配置文件
        Configuration entries = new Configuration();
        //获取客户端对象
        fileSystem = FileSystem.get(uri, entries, "yj");
        /* 创建一个文件夹 */

    }
    @Test
    public void testMkdir() throws IOException {
        boolean mkdirs = fileSystem.mkdirs(new Path("/xiyou/huaguosha"));
        System.out.println(mkdirs);
    }

    @After
    public void close() throws IOException {
        fileSystem.close();
    }

    @Test
    public void copyFromLocal() throws IOException {
        fileSystem.copyFromLocalFile(false, true, new Path("F:\\hadoop\\sunwukong.txt"), new Path("/xiyou/huaguoshan"));
    }

    @Test
    public void download() throws IOException {
        fileSystem.copyToLocalFile(false ,new Path("/sanguo/shuguo.txt"), new Path("F:\\"), false);
    }

    @Test
    public void delete()throws IOException{
        fileSystem.delete(new Path("/xiyou/huaguoshan"), true);
    }

    @Test
    public void moveOrRemove() throws IOException {
        fileSystem.rename(new Path("/xiyou/huaguosha"), new Path("/xiyou/huaguoshan"));
    }

    @Test
    public void getFileDetail()throws IOException{
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);
        while(locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            System.out.println(next.getPermission()+" "+next.getOwner()+" "+next.getGroup()+" "+next.getLen()+" "+next.getModificationTime()+" "+next.getReplication()+""+next.getBlockSize()+" "+next.getPath().getName());
            BlockLocation[] blockLocations = next.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }
    //F:\hadoop\sunwukong.txt
}
