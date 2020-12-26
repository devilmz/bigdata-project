package com.zfk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSDemo {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://cvm-172-17-0-14:9000");
        FileSystem fs = null;

        try {
            fs = FileSystem.get(new URI("hdfs://cvm-172-17-0-14:9000"),conf,"root");
//            createDir(fs);
//            getFileList(fs);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void getFileList(FileSystem fs) throws IOException {
        FileStatus[] filelists = fs.listStatus(new Path("/"));

        for (FileStatus filelist : filelists) {
            System.out.println(filelist.getPath());
        }


    }

    private static void createDir(FileSystem fs) throws IOException {
        Path path = new Path("/zfktest");
        fs.mkdirs(path);
    }

    private static void copyLocalToHdfs(FileSystem fs,String localpath,String hdfspath){
        try {
            fs.copyFromLocalFile(new Path(localpath),new Path(hdfspath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
