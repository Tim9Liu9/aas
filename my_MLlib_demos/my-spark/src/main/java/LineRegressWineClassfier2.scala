import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.SparkSession
import org.apdplat.word.WordSegmenter

/**
  * 线性回归实现分类学习
  */
object LineRegressWineClassfier2 {

    //样例类,含有指标
    case class Wine(FixedAcidity: Double, VolatileAcidity: Double, CitricAcid: Double, ResidualSugar: Double, Chlorides: Double, FreeSulfurDioxide: Double, TotalSulfurDioxide: Double, Density: Double, PH: Double, Sulphates: Double, Alcohol: Double, Quality: Double)

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("LineRegressWineClassfier")
        conf.setMaster("local[4]")

        //创建SparkSession对象，
        val sess = SparkSession.builder().config(conf).getOrCreate();
        //导入隐式对象
        import sess.implicits._


        //加载持久化的模型
//        val model = LinearRegressionModel.load("file:///d:/spark/mltest/model")
        val model = LinearRegressionModel.load("file:///E:/timliu/src/data/model")


        //创建测试数据框
        val testDF = sess.createDataFrame(Seq((5.0, Vectors.dense(7.4, 0.7, 0.0, 1.9, 0.076, 25.0, 67.0, 0.9968, 3.2, 0.68, 9.8)), (5.0, Vectors.dense(7.8, 0.88, 0.0, 2.6, 0.098, 11.0, 34.0, 0.9978, 3.51, 0.56, 9.4)), (7.0, Vectors.dense(7.3, 0.65, 0.0, 1.2, 0.065, 15.0, 18.0, 0.9968, 3.36, 0.57, 9.5)))).toDF("label", "features")


        //创建test表
        testDF.createOrReplaceTempView("test")
        //查看预测值
        val tested = model.transform(testDF).select("features", "label", "prediction")

        //测试特征向量
        val predictDF = sess.sql("SELECT features FROM test")
        val predicted = model.transform(predictDF).select("features", "prediction")
        predicted.show()
    }
}
