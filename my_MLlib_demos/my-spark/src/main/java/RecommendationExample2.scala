
/**
  * 推荐
  */
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}
// $example off$

object RecommendationExample2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CollaborativeFilteringExample")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    // 比als_test.data删除第5行数据：2,1,5.0 ；删除了第9行：3,1,1.0
//    val data = sc.textFile("file:///D:\\spark\\mltest\\mllib\\als\\test.data")
    val data = sc.textFile("file:///E:/timliu/src/data/als_test2.data")

    //变换成rationg对象
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    //发生购买行为(评分)数据
//    val realData = ratings.map(t=>(t.user,t.product))

    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10

    //模型,举证分解模型
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    //笛卡尔积
//    val allUsers = sc.makeRDD(Array(1,2,3,4))
//    val allProduct = sc.makeRDD(Array(1,2,3,4))
//    val userProducts = allUsers.cartesian(allProduct)

//    val unbuy = userProducts.subtract(realData)
//    unbuy.collect().foreach(println)


    // 抽取出(user, product)
    val usersProducts = sc.makeRDD(Array((1,1), (2,1), (3,1)))


    //预测评分.
    //val predictions = model.predict(unbuy)
    val predictions = model.predict(usersProducts)

    predictions.collect().foreach(println)

  }
}
