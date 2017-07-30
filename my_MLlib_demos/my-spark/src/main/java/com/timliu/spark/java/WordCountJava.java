package com.timliu.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Administrator on 2017/7/9.
 */
public class WordCountJava {
	public static void main(String[] args) throws IOException {
		//创建SparkConf对象
		SparkConf conf = new SparkConf();
		conf.setAppName("WordCountJava");
//		conf.setMaster("spark://s201:7077") ;
		conf.setMaster("spark://192.168.11.70:7077") ;

		//java sc对象
		JavaSparkContext sc = new JavaSparkContext(conf) ;
		//加载文件
		JavaRDD<String> rdd1 = sc.textFile("hdfs://mycluster/user/centos/1.txt");
		//压扁
		JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) throws Exception {
				String[] arr = s.split(" ");
				return Arrays.asList(arr).iterator() ;
			}
		});
		//map,标1
		JavaPairRDD<String,Integer> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2<String, Integer>(s,1);
			}
		});
		//聚合
		JavaPairRDD<String,Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		//得到集合
		List<Tuple2<String,Integer>> list = rdd4.collect();
		for(Tuple2<String,Integer> t : list){
			System.out.println(t._1 + "=" + t._2());
		}


	}
}
