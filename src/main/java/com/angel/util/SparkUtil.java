package com.angel.util;

import java.util.Collection;

/**
 * Created by dell on 2016/1/25.
 */
public class SparkUtil {
    public static <T> void print(Collection<T> c) {
        for (T t : c) {
            System.out.println(t.toString());
        }
    }

    public static <T> void saveAsFile(){

    }
}
