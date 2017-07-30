package com.timliu.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 垃圾邮件过滤 java 版本
 *
 * Created by Administrator on 2017/7/17.
 */
public class SpamFilterJava {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("SpamFilterJava");
		conf.setMaster("local[4]");
		SparkSession sess = SparkSession.builder().config(conf).getOrCreate();

		//邮件训练数据
		GenericRow row1 = new GenericRow(new Object[]{"you@example.com", "hope you are well",0.0f});
		GenericRow row2 = new GenericRow(new Object[]{"raj@example.com", "nice to hear from you",0.0f});
		GenericRow row3 = new GenericRow(new Object[]{"thomas@example.com", "happy holidays",0.0f});
		GenericRow row4 = new GenericRow(new Object[]{"mark@example.com", "see you tomorrow",0.0f});
		GenericRow row5 = new GenericRow(new Object[]{"xyz@example.com", "save money",1.0f});
		GenericRow row6 = new GenericRow(new Object[]{"top10@example.com", "low interest rate",1.0f});
		GenericRow row7 = new GenericRow(new Object[]{"marketing@example.com", "cheap loan",1.0f});


		List<Row> buf = new ArrayList<Row>();
		buf.add(row1);
		buf.add(row2);
		buf.add(row3);
		buf.add(row4);
		buf.add(row5);
		buf.add(row6);
		buf.add(row7);

		//字段数组
		StructField[] fields = {
				new StructField("email", DataTypes.StringType,true, Metadata.empty()),
				new StructField("message", DataTypes.StringType,true, Metadata.empty()),
				new StructField("label", DataTypes.FloatType,true, Metadata.empty()),
		} ;
		StructType type = new StructType(fields);
		//创建训练数据集
		Dataset<Row> training = sess.createDataFrame(buf, type);

		//分词器
		Tokenizer tokenizer = new Tokenizer().setInputCol("message").setOutputCol("words");

		//哈希词频
		HashingTF tf = new HashingTF().setNumFeatures(1000).setInputCol("words").setOutputCol("features");

		//逻辑回归对象
		LogisticRegression lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01);

		//数组
		PipelineStage[] arr = new PipelineStage[3];
		arr[0] = tokenizer ;
		arr[1] = tf ;
		arr[2] = lr ;

		Pipeline pipeline = new Pipeline().setStages(arr);

		//拟合
		PipelineModel model = pipeline.fit(training);


		//测试数据
		GenericRow test_row1 = new GenericRow(new Object[]{"you@example.com", "how are you"});
		GenericRow test_row2 = new GenericRow(new Object[]{"jain@example.com", "nice to hear from you"});
		GenericRow test_row3 = new GenericRow(new Object[]{"thomas@example.com", "want some money"});
		GenericRow test_row4 = new GenericRow(new Object[]{"zhou@example.com", "secure loan"});
		GenericRow test_row5 = new GenericRow(new Object[]{"ted@example.com", "need loan"});


		List<Row> test_buf = new ArrayList<Row>();
		test_buf.add(test_row1);
		test_buf.add(test_row2);
		test_buf.add(test_row3);
		test_buf.add(test_row4);
		test_buf.add(test_row5);

		//字段数组
		StructField[] test_fields = {
				new StructField("email", DataTypes.StringType, true, Metadata.empty()),
				new StructField("message", DataTypes.StringType, true, Metadata.empty()),
		};
		StructType test_type = new StructType(test_fields);
		Dataset<Row> test = sess.createDataFrame(test_buf, test_type);

		Dataset<Row> df1 = model.transform(test);
		Dataset<Row> df2 = df1.select("email","prediction");
		df2.show();
	}
}
