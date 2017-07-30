import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

/**
  * 聚类算法
  */
object KMeansExample {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("KMeansExample")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 加载数据
//    val data = sc.textFile("file:///E:/timliu/src/data/my-kmean.txt")
    // 结果得到的2个聚类的中心的点： [9.099999999999998, 9.099999999999998, 9.099999999999998] ;  [0.1 , 0.1, 0.1]， 方差值： 0.11999999999994547
//    val data = sc.textFile("file:///E:/timliu/src/data/kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    //聚类参数
    val numClusters = 2 // 分为2类
    val numIterations = 20  // 迭代20次
    //训练模型
    val clusters: KMeansModel = KMeans.train(parsedData, numClusters, numIterations)
    // 聚类的中心点
    val rr = clusters.clusterCenters
    rr.foreach(println)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    // 方差值： 0.11999999999994547
    println("Within Set Sum of Squared Errors = " + WSSSE)
    sc.stop()
  }
}
