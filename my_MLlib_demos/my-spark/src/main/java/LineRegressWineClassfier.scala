import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 线性回归实现分类学习
  */
object LineRegressWineClassfier {

    //样例类,含有指标
    case class Wine(FixedAcidity: Double, VolatileAcidity: Double, CitricAcid: Double, ResidualSugar: Double, Chlorides: Double, FreeSulfurDioxide: Double, TotalSulfurDioxide: Double, Density: Double, PH: Double, Sulphates: Double, Alcohol: Double, Quality: Double)

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("LineRegressWineClassfier")
        conf.setMaster("local[2]")
        //指定酒目录
        val dataDir = "file:///E:/timliu/src/data/red-wine.csv"
        println("--->1")

        //创建SparkSession对象，
        val sess = SparkSession.builder().config(conf).getOrCreate();
        println("--->1a")
        //导入隐式对象
        import sess.implicits._

        println("--->2")

        //转换数据到Wine对象
        val wineDataRDD = sess.sparkContext.textFile(dataDir).map(_.split(";")).map(w => Wine(w(0).toDouble, w(1).toDouble, w(2).toDouble, w(3).toDouble, w(4).toDouble, w(5).toDouble, w(6).toDouble, w(7).toDouble, w(8).toDouble, w(9).toDouble, w(10).toDouble, w(11).toDouble))

        //将wine对象转换层label + feature的DF.
        val trainingDF = wineDataRDD.map(w => (w.Quality, Vectors.dense(w.FixedAcidity, w.VolatileAcidity, w.CitricAcid, w.ResidualSugar, w.Chlorides, w.FreeSulfurDioxide, w.TotalSulfurDioxide, w.Density, w.PH, w.Sulphates, w.Alcohol))).toDF("label", "features")

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
        val testDF = sess.createDataFrame(Seq((5.0, Vectors.dense(7.4, 0.7, 0.0, 1.9, 0.076, 25.0, 67.0, 0.9968, 3.2, 0.68, 9.8)), (5.0, Vectors.dense(7.8, 0.88, 0.0, 2.6, 0.098, 11.0, 34.0, 0.9978, 3.51, 0.56, 9.4)), (7.0, Vectors.dense(7.3, 0.65, 0.0, 1.2, 0.065, 15.0, 18.0, 0.9968, 3.36, 0.57, 9.5)))).toDF("label", "features")


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
