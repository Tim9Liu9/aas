import LineRegressWineClassfier.Wine
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

/**
  * Created by tim on 2017/7/29.
  */
object TestLineRegressWineClassfier2 {

    //样例类,含有指标
//    case class Wine(FixedAcidity: Double, VolatileAcidity: Double, CitricAcid: Double, ResidualSugar: Double, Chlorides: Double, FreeSulfurDioxide: Double, TotalSulfurDioxide: Double, Density: Double, PH: Double, Sulphates: Double, Alcohol: Double, Quality: Double)

    //样例类,含有指标
    case class Wine(Quality: Double, a: Double, b: Double, c: Double)

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("TestLineRegressWineClassfier")
        conf.setMaster("local[4]")
        //指定酒目录

        val dataDir = "file:///E:/timliu/src/data/test-wine.csv"
        println("--->1")

        //创建SparkSession对象，
        val sess = SparkSession.builder().config(conf).getOrCreate();
        println("--->1a")
        //导入隐式对象
        import sess.implicits._

        println("--->2")

        //转换数据到Wine对象
        val wineDataRDD = sess.sparkContext.textFile(dataDir).map(_.split(";")).map(w => Wine(w(0).toDouble, w(1).toDouble, w(2).toDouble, w(3).toDouble))

        //将wine对象转换层label + feature的DF.
        val trainingDF = wineDataRDD.map(w => (w.Quality, Vectors.dense(w.a, w.b, w.c))).toDF("label", "features")

        println("--->3")

        //线性回归对象
        val lr = new LinearRegression()
        // 最多迭代10次, 默认100
        lr.setMaxIter(10)
        //训练模型
        val model = lr.fit(trainingDF)

        //持久化模型
        //model.save("file:///E:/timliu/src/data/model");

        //创建测试数据框
        val testDF = sess.createDataFrame(Seq((0.2, Vectors.dense(2, 2, 1))
            , (0.3, Vectors.dense( 3, 3, 2))
            , (0.1, Vectors.dense( 1, 1, 0.1)))).toDF("label", "features")

        //创建test表
        testDF.createOrReplaceTempView("test")
        //查看预测值
        val tested = model.transform(testDF).select("features", "label", "prediction")
        tested.show(false)

        //测试特征向量
        val predictDF = sess.sql("SELECT features FROM test")
        predictDF.show(false)


        val predicted = model.transform(predictDF).select("features", "prediction")
        predicted.show(false)
        println("--->9")
    }
}
