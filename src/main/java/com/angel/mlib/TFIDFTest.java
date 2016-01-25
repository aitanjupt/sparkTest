package com.angel.mlib;


import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Vector;

import java.io.*;
import java.lang.Iterable;
import java.util.ArrayList;

import org.nlpcn.commons.lang.tire.domain.Forest;

import java.util.List;


/*
spark-submit \
--master yarn-client --class com.angel.mlib.TFIDFTest \
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
hadoop dfs -rm -r /dw_ext/mllib/tfidf.test.out1
 */
public class TFIDFTest implements Serializable {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("TFIDFTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load and parse the data
        String path = "/dw_ext/mllib/tfidf.test";
        JavaRDD<String> data = sc.textFile(path);
        JavaRDD<Iterable<String>> documents = data.map(
                new Function<String, Iterable<String>>() {
                    public Iterable<String> call(String s) {
                        List<Term> terms = ToAnalysis.parse(s);
                        List<String> array = new ArrayList<>();
                        for (Term term : terms) {
                            String t = term.getName().trim();
                            if (!t.isEmpty()) {
                                array.add(term.getName());
                            }
                        }
                        return array;
                    }
                }
        );
        documents.repartition(1).saveAsTextFile("/dw_ext/mllib/tfidf.documents");


        Forest forest = new Forest();

        HashingTF hashingTF = new HashingTF();
        JavaRDD<Vector> tf = hashingTF.transform(documents);//算出词频
        int indexofterm = hashingTF.indexOf("使用");
        System.out.println(">>>>>>>>使用:" + indexofterm);

        indexofterm = hashingTF.indexOf("目录");
        System.out.println(">>>>>>>>目录:" + indexofterm);

        tf.repartition(1).saveAsTextFile("/dw_ext/mllib/tfidf.test.out1");
//
//        tf.cache();
//
//        IDFModel idf = new IDF().fit(tf);
//        JavaRDD<Vector> tfidf = idf.transform(tf);//使用IDF加权
//        tfidf.repartition(1).saveAsTextFile("/dw_ext/mllib/tfidf.test.out2");

//        tf.cache();
//
//        val idf = new IDF(minDocFreq = 2).fit(tf);
//        val tfidf: RDD[Vector] = idf.transform(tf)
    }

    private static void testHanziFenci() {
        String path = "D:\\tmp\\mllib/tfidf.test.hanzi";
        File file = new File(path);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                List<Term> terms = ToAnalysis.parse(tempString);
                for (Term term : terms) {
                    String t = term.getName().trim();
                    if (!t.isEmpty()) {
                        System.out.println(">>>>Name:" + t);
                    }
                }
            }
        } catch (Exception ex) {
            System.out.println(ex);
        }
    }
}

