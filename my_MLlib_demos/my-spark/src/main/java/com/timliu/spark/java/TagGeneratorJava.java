package com.timliu.spark.java;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by Administrator on 2017/7/14.
 */
public class TagGeneratorJava {
	public static void main(String[] args) {
		//创建SparkConf对象
		SparkConf conf = new SparkConf();
		conf.setAppName("TagGeneratorJava");
		conf.setMaster("local[*]");
		//java sc对象
		JavaSparkContext sc = new JavaSparkContext(conf);
		//加载文件
		JavaRDD<String> rdd1 = sc.textFile("file:///d:/spark/temptags.txt");
		//变换成元组
		JavaPairRDD<String, String> rdd2 = rdd1.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String line) throws Exception {
				String[] arr = line.split("\t");
				String busId = arr[0];
				String txt = arr[1];
				JSONObject jo = JSON.parseObject(txt);
				JSONArray jarr = jo.getJSONArray("extInfoList");
				if (jarr != null && jarr.size() > 0) {
					JSONObject v1 = jarr.getJSONObject(0);
					JSONArray arr2 = v1.getJSONArray("values");
					if (arr2 != null && arr2.size() > 0) {
						String str = "";
						int i = 0;
						while (i < arr2.size()) {
							str = str + arr2.getString(i) + ",";
							i += 1;
						}
						return new Tuple2<String, String>(busId, str.substring(0, str.length() - 1));
					} else
						return new Tuple2<String, String>(busId, "");
				} else
					return new Tuple2<String, String>(busId, "");
			}
		});

		//过滤
		JavaPairRDD<String, String> rdd3 = rdd2.filter(new Function<Tuple2<String, String>, Boolean>() {
			public Boolean call(Tuple2<String, String> t) throws Exception {
				return t._2 != null && !"".equals(t._2);
			}
		});

		//对值压扁
		JavaPairRDD<String, String> rdd4 = rdd3.flatMapValues(new Function<String, Iterable<String>>() {
			public Iterable<String> call(String str) throws Exception {
				String[] arr = str.split(",");
				return Arrays.asList(arr);
			}
		});
		//(1234-味道好 , 1)
		JavaPairRDD<String, Integer> rdd5 = rdd4.mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
			public Tuple2<String, Integer> call(Tuple2<String, String> t) throws Exception {
				return new Tuple2<String, Integer>(t._1 + "-" + t._2, 1);
			}
		});

		//(1234-味道好 , 34)
		JavaPairRDD<String, Integer> rdd6 = rdd5.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		//(1234,List((味道好,23)))
		JavaPairRDD<String, List<Tuple2<String, Integer>>> rdd7 = rdd6.mapToPair(
				new PairFunction<Tuple2<String, Integer>, String, List<Tuple2<String, Integer>>>() {
					public Tuple2<String, List<Tuple2<String, Integer>>> call(Tuple2<String, Integer> t) throws Exception {
						String[] arr = t._1().split("-");
						String busId = arr[0];
						String comm = arr[1];
						List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
						list.add(new Tuple2<String, Integer>(comm, t._2));
						return new Tuple2<String, List<Tuple2<String, Integer>>>(busId, list);
					}
				});

		//聚合
		JavaPairRDD<String, List<Tuple2<String, Integer>>> rdd8 = rdd7.reduceByKey(
				new Function2<List<Tuple2<String, Integer>>, List<Tuple2<String, Integer>>, List<Tuple2<String, Integer>>>() {
					public List<Tuple2<String, Integer>> call(List<Tuple2<String, Integer>> v1, List<Tuple2<String, Integer>> v2) throws Exception {
						v1.addAll(v2);
						return v1;
					}
				});

		//映射
		JavaRDD<Tuple2<String, List<Tuple2<String, Integer>>>> rdd9 = rdd8.map(new Function<Tuple2<String,List<Tuple2<String,Integer>>>, Tuple2<String, List<Tuple2<String, Integer>>>>() {
			public Tuple2<String, List<Tuple2<String, Integer>>> call(Tuple2<String, List<Tuple2<String, Integer>>> t) throws Exception {
				Collections.sort(t._2, new Comparator<Tuple2<String,Integer>>() {
					public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
						return -(o1._2 - o2._2);
					}
				});
				List<Tuple2<String, Integer>> top5 = null ;
				if(t._2.size() > 4){
					top5 = new ArrayList<Tuple2<String, Integer>>(t._2.subList(0,5));
				}else{
					top5 = t._2 ;
				}
				return new Tuple2<String, List<Tuple2<String, Integer>>>(t._1,top5) ;
			}
		});

		//
		JavaRDD<Tuple2<String, List<Tuple2<String, Integer>>>> rdd10 = rdd9.sortBy(new Function<Tuple2<String,List<Tuple2<String,Integer>>>, Integer>() {
			public Integer call(Tuple2<String, List<Tuple2<String, Integer>>> t) throws Exception {
				return t._2.get(0)._2;
			}
		},false,1);

		for(Object o : rdd10.collect()){
			System.out.println(o);
		}
	}
}

