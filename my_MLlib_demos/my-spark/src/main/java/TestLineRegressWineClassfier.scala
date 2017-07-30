import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 线性回归实现分类学习
  */
object TestLineRegressWineClassfier {

    //样例类,含有指标
    case class Wine(Quality: Double,a: Double, b: Double, c: Double)

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("TestLineRegressWineClassfier")
        conf.setMaster("local[2]")
        //指定酒目录
        val dataDir = "file:///E:/timliu/src/data/test-wine.csv"

        //创建SparkSession对象，
        val sess = SparkSession.builder().config(conf).getOrCreate();
        //导入隐式对象
        import sess.implicits._


        //转换数据到Wine对象
        //(q,a,b,c)
        val wineDataRDD = sess.sparkContext.textFile(dataDir).map(_.split(";")).map(w => Wine(w(0).toDouble, w(1).toDouble, w(2).toDouble,w(3).toDouble))


        //将wine对象转换层label + feature的DF.
        val trainingDF = wineDataRDD.map(w => (w.Quality, Vectors.dense(w.a , w.b , w.c))).toDF("label", "features")
        trainingDF.show()


        //线性回归对象
        val lr = new LinearRegression()
        lr.setMaxIter(10)
        //拟合训练数据
        val model = lr.fit(trainingDF)

        //持久化模型
        //model.save("file:///E:/timliu/src/data/model00");

        //创建测试数据框
//        val testDF = sess.createDataFrame(Seq((0.2, Vectors.dense(0.2, 2, 2, 1))
//            , (0.3, Vectors.dense(0.3, 3,3,2))
//            , (0.1, Vectors.dense(0.1, 1,1,0.1)))).toDF("label", "features")
        val testDF = sess.createDataFrame(Seq((0.2, Vectors.dense(2.0, 2.0, 1.0))
            , (0.3, Vectors.dense( 3.0,3.0,2.0))
            , (0.1, Vectors.dense( 1.0,1.0,0.1)))).toDF("label", "features")
        testDF.show()


        //创建test表
        testDF.createOrReplaceTempView("test")
        //查看预测值
        val tested = model.transform(testDF).select("features", "label", "prediction")
        tested.show()



        //测试特征向量
        val predictDF = sess.sql("SELECT features FROM test")
        val predicted = model.transform(predictDF).select("features", "prediction")
        predicted.show()
    }
}
