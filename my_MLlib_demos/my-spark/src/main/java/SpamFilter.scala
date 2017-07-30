
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer, StopWordsRemover, Tokenizer, Word2Vec}

/**
  * 垃圾邮件过滤
  */
object SpamFilter {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("SpamFilter")
        conf.setMaster("local[4]")

        //sparkSession
        val sess = SparkSession.builder().config(conf).getOrCreate()
        import sess.implicits._

        //邮件训练数据
        val training = sess.createDataFrame(Seq(("you@example.com", "hope you are well", 0.0),
            ("raj@example.com", "nice to hear from you", 0.0),
            ("thomas@example.com", "happy holidays", 0.0),
            ("mark@example.com", "see you tomorrow", 0.0),
            ("xyz@example.com", "save money", 1.0),
            ("top10@example.com", "low interest rate", 1.0),
            ("marketing@example.com", "cheap loan", 1.0)))
            .toDF("email", "message", "label")

        //分词
        val tokenizer = new Tokenizer().setInputCol("message").setOutputCol("words")

        //哈希词频,设置特征个数,
        val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol("words").setOutputCol("features")

        //逻辑回归对象
        val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)

        //管线
        val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

        //拟合数据，生成模型
        val model = pipeline.fit(training);

        //测试数据
        val test = sess.createDataFrame(Seq(("you@example.com", "how are you"),
            ("jain@example.com", "hope doing well"),
            ("caren@example.com", "want some money"),
            ("zhou@example.com", "secure loan"),
            ("ted@example.com", "need loan")))
            .toDF("email", "message")

        //预测测试数据
        val prediction = model.transform(test).select("email", "message", "prediction")
        prediction.show();

    }
}
