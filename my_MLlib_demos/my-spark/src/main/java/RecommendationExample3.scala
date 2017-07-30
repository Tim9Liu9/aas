
/**
  * 推荐 商品
  */
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}
// $example off$

object RecommendationExample3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CollaborativeFilteringExample")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("file:///E:/timliu/src/data/sample_movielens_ratings.txt")

    //变换成rationg对象
    val ratings = data.map(_.split("::") match { case Array(user, item, rate,_) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    //训练模型,矩阵分解模型
    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    //找出所有影片id
    val allMovieIds = ratings.map(t=>t.product).distinct()
    //找出1号用户看了那些影片
    val moveIds_1 = ratings.filter(t=>t.user == 1).map(t=>t.product)
    //
    val unwatchMovieIds = allMovieIds.subtract(moveIds_1).map(t=>(1,t))

    //预测评分.
    val predictions = model.predict(unwatchMovieIds)

    val resArr = predictions.collect()
    val finalArr = resArr.sortBy(t=>t.rating).reverse.take(10)
    finalArr.foreach(println)
  }
}
