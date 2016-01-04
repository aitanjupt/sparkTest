package com.angel.mlib;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/***
 * spark-submit --master yarn-client --class com.angel.mlib.FPGrowthTestReal --jars lib/hbase-client-0.98.6-cdh5.3.6.jar,lib/hbase-common-0.98.6-cdh5.3.6.jar,lib/hbase-protocol-0.98.6-cdh5.3.6.jar,lib/hbase-server-0.98.6-cdh5.3.6.jar,lib/htrace-core-2.04.jar,lib/zookeeper.jar,lib/spark-mllib_2.10-1.5.2.jar,lib/spark-core_2.10-1.5.2.jar,lib/hive-exec-0.13.1-cdh5.3.6.jar,lib/hive-serde-0.13.1-cdh5.3.6.jar spark-test-1.0.jar
 */
public class FPGrowthTestReal implements Serializable {

    static JavaSparkContext sc;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Collaborative Filtering Example");
        sc = new JavaSparkContext(conf);

        //Step1:从Hive-hdfs上读取近期的solditems数据
        JavaPairRDD<String, List<String>> transactions = null;
        List<JavaPairRDD<String, List<String>>> list_transactions = new ArrayList<>();
        try {
            getTransactionsFromFolder("/dc_ext/xbd/dm/mds/mds_dm_solditems", list_transactions);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (JavaPairRDD<String, List<String>> tran : list_transactions) {
            if (null == transactions) {
                transactions = tran;
            } else {
                transactions = transactions.union(tran);
            }
        }

        //Step2:将数据按商户进行切分
        List<String> keys = transactions.keys().distinct().collect();
        int i = 0;
        for (String key : keys) {
            i++;
            if (i == 50) {
                return;
            }
            JavaRDD<List<String>> rddByKey = getRddByKey(transactions, key);
            //Step3:在商户中的solditems数据集内进行FPGrowth关联规则挖掘
            fPGrowth(key, rddByKey);
        }
    }

    private static void fPGrowth(String businessId, JavaRDD<List<String>> transactions) {
        //.setMinSupport(0.05)过滤掉出现频率较低的数据
        FPGrowth fpg = new FPGrowth().setMinSupport(0.03).setNumPartitions(10);
        FPGrowthModel<String> model = fpg.run(transactions);    //transactions: item,item,item

//        List<FPGrowth.FreqItemset<String>> list_fi = model.freqItemsets().toJavaRDD().collect();
//        System.out.println("list_fi.size: " + list_fi.size());

//        for (FPGrowth.FreqItemset<String> itemset : list_fi) {
//            System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
//        }

        double minConfidence = 0.3;
        JavaRDD<AssociationRules.Rule<String>> rdd_rule = model.generateAssociationRules(minConfidence).toJavaRDD();
        writeRDD(businessId, rdd_rule);
    }

    private static void writeRDD(final String businessId, JavaRDD<AssociationRules.Rule<String>> rdd_rule) {
        JavaRDD<String> fileRDD = rdd_rule.map(new Function<AssociationRules.Rule<String>, String>() {
            @Override
            public String call(AssociationRules.Rule<String> rule) throws Exception {
                String ret = businessId + "\001" + rule.javaAntecedent() + "\001" + rule.javaConsequent() + "\001" + rule.confidence();
                return ret;
            }
        });
        fileRDD.repartition(1).saveAsTextFile("/dw_ext/testFPGout");
    }

    private static <K, V> JavaRDD getRddByKey(JavaPairRDD<K, List<V>> pairRDD, final K key) {
        return pairRDD.filter(new Function<Tuple2<K, List<V>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<K, List<V>> tuple2) throws Exception {
                if (tuple2._1.equals(key)) {
                    return true;
                } else {
                    return false;//去除
                }
            }
        }).values();
    }

    private static void getTransactionsFromFolder(String folderPath, List<JavaPairRDD<String, List<String>>> retTransactions) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] status = fs.listStatus(new Path(folderPath));
        if (null == status) {
            return;
        }
        for (FileStatus file : status) {
            String filePath = file.getPath().toString();
            if (file.isDirectory()) {
                getTransactionsFromFolder(filePath, retTransactions);
            } else {
                JavaPairRDD<String, List<String>> transactionsFromFile = getTransactionsFromFile(filePath);
                retTransactions.add(transactionsFromFile);
            }
        }
    }


    private static JavaPairRDD<String, List<String>> getTransactionsFromFile(String filePath) throws IOException {
        JavaPairRDD<LongWritable, BytesRefArrayWritable> rowPairRDD = sc.hadoopFile(filePath, RCFileInputFormat.class, LongWritable.class, BytesRefArrayWritable.class);
        JavaPairRDD<String, List<String>> rowRDD = rowPairRDD.mapToPair(new PairFunction<Tuple2<LongWritable, BytesRefArrayWritable>, String, List<String>>() {
            @Override
            public Tuple2<String, List<String>> call(Tuple2<LongWritable, BytesRefArrayWritable> sourceTuple2) throws Exception {
                BytesRefArrayWritable braw = sourceTuple2._2;
                Text txt = new Text();
                List<String> ls = new ArrayList<>();
                for (int i = 0; i < braw.size(); i++) {
//                    StringBuffer sb = new StringBuffer();
                    BytesRefWritable v = braw.get(i);
                    txt.set(v.getData(), v.getStart(), v.getLength());
//                    if (i == braw.size() - 1) {
//                        ls.add(txt.toString());
////                        sb.append(txt.toString());
//                    }
//                    else {
                    ls.add(txt.toString());
//                    }
                }
                Tuple2<String, List<String>> retTuple2 = new Tuple2<>(ls.get(0), ls);
                System.out.println("------------" + ls.size() + "getTransactionsFromFile key: " + ls.get(0));
                return retTuple2;
            }
        });
        return rowRDD;
    }

}