package com.querenjie.examples.bean;

import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class VoidFunctionImpl1 implements VoidFunction<Tuple2<String, Integer>> {
    private List<String> resultList = new ArrayList<String>();
    private ThreadLocal<List<String>> threadLocal = null;

    public List<String> getResultList() {
        return resultList;
    }

    public VoidFunctionImpl1(ThreadLocal threadLocal) {
        this.threadLocal = threadLocal;
    }

    public void call(Tuple2<String, Integer> tuple) throws Exception {
//        System.out.println(tuple._1 + "\t" + tuple._2);
        resultList.add(tuple._1 + "\t" + tuple._2);
    }
}
