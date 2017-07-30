import com.timliu.debugtool.RTUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Scala版的wordcount
  */
object TestSQLScala {

    //注意样例类需要定义在方法外部。
    case class Customer(id: Int, name: String, age: Int)

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("TestSQLScala")
        conf.setMaster("yarn")
        val sess = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
        //注册函数
        sess.sqlContext.sql("create function getdaybegin as 'com.timliu.applogs.udf.DayBeginUDF'");

        val df = sess.sqlContext.sql("select appid,deviceid,getdaybegin() from applogsdb.ext_startup_logs")

//        df.foreach(row => {
//            val ts = row.get(3)
//            RTUtil.sendInfo("s204", 8888, this, "" + ts, "")
//        })
        sess.close()
    }
}
