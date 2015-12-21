package com.angel.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by dell on 2015/8/11.
 */
public class Main {
    public static void main(String[] args) throws Exception {

        JavaSparkContext ctx = new JavaSparkContext("spark://192.168.181.190:7077", "WordCount",
                "/opt/cloudera/parcels/CDH/lib/spark/bin",
                "D:/Program Files/repository/com/angel/spark-test/1.0/spark-test-1.0.jar");
        JavaRDD<String> lines = ctx.textFile("D:/tmp/supervisord.log", 1);
    }

    private void sparkES(){
        JavaSparkContext ctx = new JavaSparkContext("spark://192.168.181.190:7077", "mySparkESJob",
                "/opt/cloudera/parcels/CDH/lib/spark/bin",
                "D:/Program Files/repository/com/angel/spark-test/1.0/spark-test-1.0.jar");
        JavaRDD<String> lines = ctx.textFile("D:/tmp/supervisord.log", 1);
    }

    private void hqlSpark(){
        SparkConf sparkConf = new SparkConf().setAppName("myHqlSpark");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    }

//    public static String startJob() throws Exception {
//        Job job = Job.getInstance();
//        job.setJobName("xxxx");
//        /***************************
//         *......
//         *MapReduceһ
//         *......
//         ***************************/
//
//        //
//        Configuration conf = job.getConfiguration();
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("hbase.zookeeper.quorum", "MASTER:2181");
//        conf.set("fs.default.name", "hdfs://MASTER:8020");
//        conf.set("yarn.resourcemanager.resource-tracker.address", "MASTER:8031");
//        conf.set("yarn.resourcemanager.address", "MASTER:8032");
//        conf.set("yarn.resourcemanager.scheduler.address", "MASTER:8030");
//        conf.set("yarn.resourcemanager.admin.address", "MASTER:8033");
//        conf.set("yarn.application.classpath", "$HADOOP_CONF_DIR,"
//                +"$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,"
//                +"$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,"
//                +"$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,"
//                +"$YARN_HOME/*,$YARN_HOME/lib/*,"
//                +"$HBASE_HOME/*,$HBASE_HOME/lib/*,$HBASE_HOME/conf/*");
//        conf.set("mapreduce.jobhistory.address", "MASTER:10020");
//        conf.set("mapreduce.jobhistory.webapp.address", "MASTER:19888");
//        conf.set("mapred.child.java.opts", "-Xmx1024m");
//
//        job.submit();
//        //
//        return job.getJobID().toString();
//    }
}
