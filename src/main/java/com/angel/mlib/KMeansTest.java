package com.angel.mlib;


import com.angel.util.SparkUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import java.io.Serializable;
import java.util.Collection;
import java.util.regex.Pattern;


/*
spark-submit \
--master yarn-client --class com.angel.mlib.KMeansTest \
--files hive-site.xml \
--driver-class-path ${jar_dir}/spark-assembly-1.6.0-hadoop2.2.0.jar\
:${jar_dir}/jdo-api-3.0.1.jar:${jar_dir}/derby-10.10.1.1.jar\
:${jar_dir}/datanucleus-api-jdo-3.2.6.jar:${jar_dir}/datanucleus-core-3.2.10.jar:${jar_dir}/datanucleus-rdbms-3.2.9.jar \
--jars ${jar_dir}/etl-util-1.0.jar,${jar_dir}/elasticsearch-1.7.1.jar,${jar_dir}/lucene-core-4.10.4.jar\
,${jar_dir}/hbase-client-0.98.6-cdh5.3.6.jar,${jar_dir}/hbase-common-0.98.6-cdh5.3.6.jar\
,${jar_dir}/hbase-protocol-0.98.6-cdh5.3.6.jar,${jar_dir}/hbase-server-0.98.6-cdh5.3.6.jar\
,${jar_dir}/htrace-core-2.04.jar,${jar_dir}/hive-serde-0.13.1-cdh5.3.6.jar\
,${jar_dir}/zookeeper.jar\
,${jar_dir}/ansj_seg-3.3.jar,${jar_dir}/nlp-lang-1.1.jar \
spark-test-1.0.jar
 */
/*
hadoop dfs -rm -r /dw_ext/mllib/kmean.test.out1
 */
public class KMeansTest implements Serializable {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("KMeansTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load and parse the data
        JavaRDD<String> data = sc.textFile("/dw_ext/mllib/kmean.test");
        JavaRDD<Vector> parsedData = data.map(new Function<String, Vector>() {
            @Override
            public Vector call(String s) throws Exception {
                String[] sarray = s.split(" ");
                double[] values = new double[sarray.length];
                for (int i = 0; i < sarray.length; i++)
                    values[i] = Double.parseDouble(sarray[i]);
                return Vectors.dense(values);
            }
        });

        // Cluster the data into two classes using KMeans
        int numClusters = 3;
        int numIterations = 20;
        final KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

        //打印出中心点
        System.out.println("Cluster centers:");
        for (Vector center : clusters.clusterCenters()) {
            System.out.println(" " + center);
        }

        //计算测试数据分别属于那个簇类
        JavaRDD<String> predict = parsedData.map(new Function<Vector, String>() {
            @Override
            public String call(Vector vector) throws Exception {
                String ret = vector.toString() + " belong to cluster :" + clusters.predict(vector);
                return ret;
            }
        });

        SparkUtil.print(predict.collect());

//        // Save and load model
//        clusters.save(sc.sc(), "myModelPath");
//        KMeansModel sameModel = KMeansModel.load(sc.sc(), "myModelPath");
    }

}

