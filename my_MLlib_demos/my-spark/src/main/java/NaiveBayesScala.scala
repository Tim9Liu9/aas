/**
  * Created by Administrator on 2017/7/18.
  */
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession
object NaiveBayesScala {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("NaiveBayesExample")
            .master("local[4]")
            .getOrCreate()

        // 加载数据
        val data = spark.read.format("libsvm").load("file:///E:/timliu/src/data/sample_libsvm_data.txt")

        //切割数据集，产生训练数据和测试数据
        val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 1234L)

        // 训练贝叶斯模型
        val model = new NaiveBayes().fit(trainingData)



        // 预测
        val predictions = model.transform(testData)
        predictions.show()

        // Select (prediction, true label) and compute test error
        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)
        println("Test set accuracy = " + accuracy)
        spark.stop()
    }
}
