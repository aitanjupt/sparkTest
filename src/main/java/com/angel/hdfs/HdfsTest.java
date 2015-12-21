package com.angel.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by dell on 2015/9/16.
 */
public class HdfsTest {

    public static void main(String[] args) throws IOException, URISyntaxException {
        try {
            System.setProperty("HADOOP_USER_NAME", "deploy");
//        String s = System.getProperty("HADOOP_USER_NAME");
            //������������ʾĿ¼�������ļ�
//        ListDirAll("hdfs://192.168.181.196:8020/");
            String fileWrite = "hdfs://192.168.181.196:8020/test2";
            String words = "This words is to write into file!\n";
            WriteToHDFS(fileWrite, words);
        }catch (Exception ex){
            System.out.println(ex);
        }
//        //�������Ƕ�ȡfileWrite�����ݲ���ʾ���ն�
//        ReadFromHDFS(fileWrite);
//        //����ɾ�������fileWrite�ļ�
//        DeleteHDFSFile(fileWrite);
        //���豾����һ��uploadFile�������ϴ����ļ���HDFS
//      String LocalFile = "file:///home/kqiao/hadoop/MyHadoopCodes/uploadFile";
//      UploadLocalFileHDFS(LocalFile, fileWrite    );
    }

    public static void ListDirAll(String DirFile) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(DirFile), conf);
        Path path = new Path(DirFile);

        FileStatus[] status = fs.listStatus(path);
        //����1
        for (FileStatus f : status) {
            System.out.println(f.getPath().toString());
        }
        //����2
        Path[] listedPaths = FileUtil.stat2Paths(status);
        for (Path p : listedPaths) {
            System.out.println(p.toString());
        }
    }

    //��ָ��λ���½�һ���ļ�����д���ַ�
    public static void WriteToHDFS(String file, String words) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataOutputStream out = fs.create(path);   //�����ļ�

        //���������������ļ�д�룬����һ���ʹ�ú���
        out.writeBytes(words);
        out.write(words.getBytes("UTF-8"));

        out.close();
        //�����Ҫ����������д�룬���Ǵ�һ���ļ�д����һ���ļ�����ʱ�����������������ݵ��ļ���
        //����ʹ������IOUtils.copyBytes������
        //FSDataInputStream in = fs.open(new Path(args[0]));
        //IOUtils.copyBytes(in, out, 4096, true)        //4096Ϊһ�θ��ƿ��С��true��ʾ������ɺ�ر���
    }


}
