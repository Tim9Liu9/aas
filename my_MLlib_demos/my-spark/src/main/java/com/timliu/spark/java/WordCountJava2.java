package com.timliu.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Administrator on 2017/7/9.
 */
public class WordCountJava2 {
	public static void main(String[] args) {
		//创建SparkConf对象
		SparkConf conf = new SparkConf();
		conf.setAppName("WordCountJava");
		conf.setMaster("local[*]") ;

		//java sc对象
		JavaSparkContext sc = new JavaSparkContext(conf) ;
		//加载文件
		JavaRDD<String> rdd1 = sc.textFile("d:/1.txt");
		//压扁
		JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) throws Exception {
				String[] arr = s.split(" ");
				return Arrays.asList(arr).iterator() ;
			}
		});

		JavaRDD<Tuple2<String,Integer>> rdd3 = rdd2.map(new Function<String, Tuple2<String,Integer>>() {
			public Tuple2<String, Integer> call(String v1) throws Exception {
				return new Tuple2<String, Integer>(v1,1);
			}
		});

		//分组
		JavaPairRDD<String,Iterable<Tuple2<String,Integer>>> rdd4 = rdd3.groupBy(new Function<Tuple2<String,Integer>, String>() {
			public String call(Tuple2<String, Integer> v1) throws Exception {
				return v1._1();
			}
		});



	}
}
