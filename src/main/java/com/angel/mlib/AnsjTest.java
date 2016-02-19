package com.angel.mlib;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dell on 2016/2/4.
 */
public class AnsjTest {

    public static void main(String[] args) {
        String s = "“黑大巴”核载55人塞了87人 涉嫌危险驾驶罪司机..";
        List<Term> terms = ToAnalysis.parse(s);
        List<String> array = new ArrayList<>();
        for (Term term : terms) {
            if (term.getNatureStr().equals("v") || term.getNatureStr().equals("n"))
            {
                System.out.println();
            }
            String t = term.getName().trim();
            if (!t.isEmpty()) {
                array.add(t);
            }
        }
    }

}
