import org.apache.spark.{SparkConf, SparkContext}
import com.timliu.debugtool.RTUtil

/**
  * Scala版的wordcount
  */
object TestDeploy {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("TestDeploy")
        conf.setMaster("yarn")
        val sc = new SparkContext(conf)
        RTUtil.sendInfo("s204", 8888, this, "xxx", "")
        val rdd1 = sc.makeRDD(1 to 10)
        val rdd2 = rdd1.map(e=>{
            RTUtil.sendInfo("s204",8888,this,"map","")
        })
        rdd2.collect().foreach(println)
    }
}
