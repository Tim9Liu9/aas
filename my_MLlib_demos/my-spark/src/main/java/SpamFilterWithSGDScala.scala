import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD

/**
  * SGD :Stochastic Gradient Descent,随机梯度下降.
  */
object SpamFilterWithSGDScala {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("SpamFilterWithSGDScala")
        conf.setMaster("local[4]")
        val sc = new SparkContext(conf) ;


        //加载垃圾邮件的训练数据
        val spam = sc.textFile("file:///E:/timliu/src/data/spam.txt")
        //加载非垃圾邮件的训练数据
        val normal = sc.textFile("file:///E:/timliu/src/data/ham.txt")

        //创建哈希词频，使用10000特征值
        val tf = new HashingTF(numFeatures = 10000)

        //切割文本,映射到向量.
        val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
        val normalFeatures = normal.map(email => tf.transform(email.split(" ")))

        //创建标签点数据,创建标签点对象
        val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
        val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))

        //对两种邮件进行合并,形成训练数据
        val trainingData = positiveExamples.union(negativeExamples)

        //使用随机梯度下降法训练逻辑回归模型
        val model = new LogisticRegressionWithSGD().run(trainingData)


        //准备测试数据集
        val posTest = tf.transform(
            "O M G GET cheap stuff by sending money to ...".split(" "))
        val negTest = tf.transform(
            "Hi Dad, I started studying Spark the other ...".split(" "))

        //预测
        println("Prediction for positive test example: " + model.predict(posTest))
        println("Prediction for negative test example: " + model.predict(negTest))













    }
}
