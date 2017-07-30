
/**
  * 推荐
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
// $example off$

object  RecommendationExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CollaborativeFilteringExample")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("file:///E:/timliu/src/data/als_test.data")

    //变换成rationg对象
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10

    //模型,举证分解模型
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    //抽取出(user,product)  用户、商品、评分
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }

    //预测评分.
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }

    //取出原来的真实评分和预测评分做连接操作。
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    ratesAndPreds.collect().foreach(println)

    //计算误差,均方差
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    println("Mean Squared Error = " + MSE)
  }
}
