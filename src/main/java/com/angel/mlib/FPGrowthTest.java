package com.angel.mlib;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/***
 * 测试是否可以跨区域推荐
 * 用户1与其他用户的商品不在一个区域内
 */
public class FPGrowthTest implements Serializable {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Collaborative Filtering Example");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load and parse the data
        String path = "/dw_ext/FPGrowth20.train";
        JavaRDD<String> data = sc.textFile(path);


        JavaRDD<List<String>> transactions = data.map(
                new Function<String, List<String>>() {
                    public List<String> call(String line) {
                        String[] parts = line.split(",");
                        return Arrays.asList(parts);
                    }
                }
        );
        //.setMinSupport(0.05)过滤掉出现频率较低的数据
        FPGrowth fpg = new FPGrowth().setMinSupport(0.03).setNumPartitions(10);
        FPGrowthModel<String> model = fpg.run(transactions);

        List<FPGrowth.FreqItemset<String>> list_fi = model.freqItemsets().toJavaRDD().collect();
        System.out.println("list_fi.size: " + list_fi.size());

        for (FPGrowth.FreqItemset<String> itemset : list_fi) {
            System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
        }

        double minConfidence = 0.3;

        List<AssociationRules.Rule<String>> list_rule = model.generateAssociationRules(minConfidence).toJavaRDD().collect();
        System.out.println("list_rule.size: " + list_rule.size());
        for (AssociationRules.Rule<String> rule : list_rule) {
            System.out.println(
                    rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
        }

    }
}