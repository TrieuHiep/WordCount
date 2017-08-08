package com.tatsuya.RDD;

import com.tatsuya.X;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class WordCount {
    private static final Pattern SPACE = Pattern.compile(" ");
    public static void main(String[] args) {

        SparkConf sConf = new SparkConf().setAppName("WordCount.main").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sConf);
        JavaRDD<String> lines = sc.textFile("/home/hiept/HelloSpark/largeText.txt");

        /*
        1 cach khac
        JavaRDD<String> lines1 = sc.parallelize(new Vector<T>(Arrays.asList("1","2","3")));
        */

//        JavaPairRDD<String,String> files = sc.wholeTextFiles("/home/tatsuya/");
//        files.collect().forEach(new Consumer<Tuple2<String, String>>() {
//            @Override
//            public void accept(Tuple2<String, String> stringStringTuple2) {
//                System.out.println(stringStringTuple2._1());
//            }
//        });

//		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
//
//	    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
//
//	    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
//
//	    List<Tuple2<String, Integer>> output = counts.collect();
//	    for (Tuple2<?,?> tuple : output) {
//	      System.out.println(tuple._1() + ": " + tuple._2());
//	    }

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> list = new Vector<>();
                StringTokenizer stk = new StringTokenizer(s," ,.:--;'=_#?()!");
                while(stk.hasMoreTokens()){
                    list.add(stk.nextToken().toLowerCase());
                }
                return list.iterator();
            }
        });

       JavaPairRDD<String,Integer> ones =  words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        });

       JavaPairRDD<String,Integer> count = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
           @Override
           public Integer call(Integer i1, Integer i2) throws Exception {
               return i1+i2;
           }
       });
//       count.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//           @Override
//           public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//               System.out.println(stringIntegerTuple2._1()+" - "+stringIntegerTuple2._2());
//           }
//       });

        JavaPairRDD<String,Integer> sortedCount = count.sortByKey(new X());
        List<Tuple2<String,Integer>> res  = sortedCount.collect();
        res.forEach(t->{
            System.out.println(t._1()+" - "+t._2());
        });
       sc.close();
    }
}
