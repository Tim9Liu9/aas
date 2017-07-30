import org.apache.spark.{SparkConf, SparkContext}

/**
  * Scala版的wordcount
  */
object WorldCountScala2 {
    def main(args: Array[String]): Unit = {
        //创建SparkConf对象
        val conf = new SparkConf()
        conf.setAppName("wordCountScala")
        conf.setMaster("local[*]")
        //        val value = conf.get("spark.default.parallelism");
        //        println(value)

        //创建SparkContext对象
        val sc = new SparkContext(conf)
        var r = sc.textFile("file:///d:/1.txt")
            .repartition(8)
            .flatMap(_.split(" "))
            .repartition(7)
            .map(w => (w + "_" + new java.util.Random().nextInt(2), 1))
            .repartition(6)
            .reduceByKey(_ + _)
            .repartition(5)
            .map(t => (t._1.split("_")(0), t._2))
            .repartition(4)
            .reduceByKey(_ + _)
            .repartition(3)
            .collect;
        println(r)
        while (true) {
            Thread.sleep(2000)
        }
    }
}
