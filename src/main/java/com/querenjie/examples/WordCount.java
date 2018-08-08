package com.querenjie.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *  一个统计文件中每个单词的数量的例子程序
 */
public class WordCount {
    public static void main(String[] args) {
        long startTimeMillis = System.currentTimeMillis();
        //要读取的文件
        //String logFile = "D:\\test\\hadoop_data\\wordcount_data\\in\\inputFile1.txt";
        String logFile = "hdfs://192.168.1.20:9000/test/wordcount/in/inputFile1.txt";
        //放到集群中运行时不要用setMaster
//        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wc");
        SparkConf conf = new SparkConf().setAppName("wc");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> text = sc.textFile(logFile);
        //文件中的内容全部切分成单词
        JavaRDD<String> words = text.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        //每个单词都对应初始值计数为1
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        //已单词为分组统计出每个单词的使用频率
        JavaPairRDD<String, Integer> results = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer value1, Integer value2) throws Exception {
                return value1 + value2;
            }
        });
        //对单词和词频元祖内容进行调换，变成（词频，单词）
        JavaPairRDD<Integer, String> temp = results.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<Integer, String>(tuple2._2, tuple2._1);
            }
        });
        //对词频进行倒排序
        temp = temp.sortByKey(false);
        //重新将元祖内容进行互换
        results = temp.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                return new Tuple2<String, Integer>(tuple2._2, tuple2._1);
            }
        });
        //打印结果
        if (results != null) {
            List<Tuple2<String, Integer>> listResult = results.collect();
            for (Tuple2<String, Integer> tuple : listResult) {
                System.out.println(tuple._1 + "\t" + tuple._2);
            }
        }
//        results.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//
//            public void call(Tuple2<String, Integer> tuple) throws Exception {
//                resultList.add(tuple._1 + "\t" + tuple._2);
//            }
//        });

        long endTimeMillis = System.currentTimeMillis();
        System.out.println("总耗时：" + (endTimeMillis - startTimeMillis) + "毫秒");

        sc.close();
    }
}
