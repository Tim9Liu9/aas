package com.timliu.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * java版
 */
public class SparkStreamingJava {
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf();
		conf.setAppName("SparkStreamingJava");
		conf.setMaster("local[4]");
		//创建流上下文
		JavaStreamingContext ssc = new JavaStreamingContext(conf, Seconds.apply(5));

		JavaDStream<String> lines = ssc.socketTextStream("localhost",8888);
		//压扁
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) throws Exception {
				return Arrays.asList(s.split(" ")).iterator();
			}
		});
		//标1成对
		JavaPairDStream<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2<String, Integer>(s,1);
			}
		});
		//按照key聚合
		JavaPairDStream result = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		result.print();
		ssc.start();
		ssc.awaitTermination();
	}
}