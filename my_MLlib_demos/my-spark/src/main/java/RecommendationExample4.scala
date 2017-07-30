
/**
  * 推荐用户
  */
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}
// $example off$

object RecommendationExample4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CollaborativeFilteringExample")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("file:///E:/timliu/src/data/sample_movielens_ratings.txt",1)

    //变换成rationg对象
    val ratings = data.map(_.split("::") match { case Array(user, item, rate,_) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    //训练模型,矩阵分解模型
    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    //找出所有影片id
    val allUserIds = ratings.map(t=>t.user).distinct()
    //找出1号用户看了那些影片
    val userIds_1 = ratings.filter(t=>t.product == 1).map(t=>t.user)
    //
    val unwatchUserIds = allUserIds.subtract(userIds_1).map(t=>(t,1))

    //预测评分.
    val predictions = model.predict(unwatchUserIds)

    val resArr = predictions.collect()
    val finalArr = resArr.sortBy(t=>t.rating).reverse.take(10)
    finalArr.foreach(println)
  }
}
