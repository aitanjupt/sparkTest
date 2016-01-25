package com.angel.mlib;


import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;


/*
spark-submit \
--master yarn-client --class com.angel.mlib.KMeansTfidfTest \
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
public class KMeansTfidfTest implements Serializable {

    static SparkConf conf = new SparkConf().setAppName("KMeansTest");
    static final transient JavaSparkContext sc = new JavaSparkContext(conf);

    public static void main(String[] args) {

        // Load and parse the data
        JavaRDD<String> data = sc.textFile("/dw_ext/mllib/kmean-tfidf.order.test");
        JavaPairRDD<String, Tuple2<String, Vector>> allData = data.mapToPair(new PairFunction<String, String, Tuple2<String, Vector>>() {
            @Override
            public Tuple2<String, Tuple2<String, Vector>> call(String s) throws Exception {
                String[] sarray = s.split("\t");
                String shopId = sarray[0];
                String itemName = sarray[1];
                double count = Double.parseDouble(sarray[2]);
                Tuple2<String, Tuple2<String, Vector>> ret = new Tuple2<>(shopId, new Tuple2<>(itemName, Vectors.dense(count)));
                return ret;
            }
        });

        List<String> allShops = allData.keys().distinct().collect();
        JavaRDD<String> hotNamesAll = null;
        for (final String shop : allShops) {
            JavaPairRDD<String, Tuple2<String, Vector>> items = allData.filter(new Function<Tuple2<String, Tuple2<String, Vector>>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, Tuple2<String, Vector>> stringTuple2Tuple2) throws Exception {
                    if (stringTuple2Tuple2._1.equals(shop)) {
                        return true;
                    } else {
                        return false;//去除
                    }
                }
            });
            JavaRDD<Vector> vectors = items.map(new Function<Tuple2<String, Tuple2<String, Vector>>, Vector>() {
                @Override
                public Vector call(Tuple2<String, Tuple2<String, Vector>> stringTuple2Tuple2) throws Exception {
                    return stringTuple2Tuple2._2._2;
                }
            });
            double threshold = getHotThreshold(vectors);
            JavaRDD<String> hotNames = getHotNames(JavaPairRDD.fromJavaRDD(items.values()), threshold);

            if (null == hotNamesAll)
                hotNamesAll = hotNames;
            else
                hotNamesAll = hotNamesAll.union(hotNames);
        }
        doTFIDF3(hotNamesAll);
    }

//    应该直接把所有的热销商品的order_itemname拉下，作词频。



//    private static void doTFIDF(JavaRDD<String> hotNames) {
//
//        JavaRDD<String> documentsParsed = hotNames.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterable<String> call(String s) throws Exception {
//                List<Term> terms = ToAnalysis.parse(s);
//                List<String> array = new ArrayList<>();
//                for (Term term : terms) {
//                    String t = term.getName().trim();
//                    if (!t.isEmpty()) {
//                        array.add(term.getName());
//                    }
//                }
//                return array;
//            }
//        });
//        HashingTF hashingTF = new HashingTF();
//        Vector tf = hashingTF.transform(documentsParsed.collect());//算出词频
//        System.out.println(tf.toString());
//        IDFModel idf = new IDF().fit(tf);
//        JavaRDD<Vector> tfidf = idf.transform(tf);//使用IDF加权
//        System.out.println(tfidf.toString());
//    }

    private static void doTFIDF2(JavaRDD<String> hotNames) {
        JavaRDD<Iterable<String>> documentsParsed = hotNames.flatMap(new FlatMapFunction<String, Iterable<String>>() {
            @Override
            public Iterable<Iterable<String>> call(String s) throws Exception {
                List<Term> terms = ToAnalysis.parse(s);
                List<Iterable<String>> array = new ArrayList<>();
                for (Term term : terms){
                    String t = term.getName().trim();
                    if (!t.isEmpty()) {
                        ArrayList<String> al = new ArrayList();
                        al.add(term.getName());
                        array.add(al);
                    }
                }
                return array;
            }
        });
        HashingTF hashingTF = new HashingTF();
        JavaRDD<Vector> tf = hashingTF.transform(documentsParsed);//算出词频

        IDFModel idf = new IDF().fit(tf);
        JavaRDD<Vector> tfidf = idf.transform(tf);//使用IDF加权

        print(tf.collect());
        print(tfidf.collect());
    }

    private static void doTFIDF3(JavaRDD<String> hotNames) {
        JavaRDD<Iterable<String>> documentsParsed = hotNames.map(new Function<String, Iterable<String>>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                List<Term> terms = ToAnalysis.parse(s);
                List<String> array = new ArrayList<>();
                for (Term term : terms) {
                    String t = term.getName().trim();
                    if (!t.isEmpty()) {
                        ArrayList<String> al = new ArrayList();
                        array.add(term.getName());
                    }
                }
                return array;
            }
        });
        HashingTF hashingTF = new HashingTF();
        JavaRDD<Vector> tf = hashingTF.transform(documentsParsed);//算出词频

        IDFModel idf = new IDF().fit(tf);
        JavaRDD<Vector> tfidf = idf.transform(tf);//使用IDF加权

        print(tf.collect());
        print(tfidf.collect());
    }


    private static JavaRDD<String> getHotNames(JavaPairRDD<String, Vector> items, final double threshold) {
        JavaPairRDD<String, Vector> hotItems = items.filter(new Function<Tuple2<String, Vector>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Vector> stringVectorTuple2) throws Exception {
                Double value = stringVectorTuple2._2.toArray()[0];
                if (0 > value.compareTo(threshold)) {//过滤只买1次的rating
                    return false;//去除
                }
                return true;
            }
        });
        return hotItems.keys();
    }

    private static double getHotThreshold(JavaRDD<Vector> vectors) {
        int numClusters = 3;
        int numIterations = 20;
        final KMeansModel clusters = KMeans.train(vectors.rdd(), numClusters, numIterations);
        Vector[] clusterCenters = clusters.clusterCenters();
        Double maxValue = 0d;
        for (Vector vector : clusterCenters) {
            Double value = vector.toArray()[0];
            if (0 > maxValue.compareTo(value)) {
                maxValue = value;
            }
        }
        return maxValue;
    }

    public static <T> void print(Collection<T> c) {
        for (T t : c) {
            System.out.println(t.toString());
        }
    }


}

