import com.alibaba.fastjson.JSON
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 标签生成器
  */
object SparkStreamingScala {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("SparkStreamingDemo")
        //注意线程数
        conf.setMaster("local[4]")

        //创建流环境
        val ssc = new StreamingContext(conf,Seconds(1));

        //创建socket Text流
        val lines = ssc.socketTextStream("localhost", 8888);

        val words = lines.flatMap(_.split(" "))

        val pairs = words.map((_,1))
        //每2两秒计算一次，每次计算3秒内的数据，存在一秒的交叉。
        //pairs.reduceByKey((a: Int, b: Int) => (a + b), Seconds(3), Seconds(2))

        //windows窗口操作，就有一般性，先设定窗口长度和滑动间隔
//        val pairWindows = pairs.window(Seconds(3), Seconds(2))
//        val result = pairWindows.reduceByKey((a: Int, b: Int) => (a + b))

        ssc.checkpoint("file:///d:/spark/chk");
        pairs.countByWindow(Seconds(3), Seconds(2)).print()

        //定义函数
        def f1(a:Seq[Int],b:Option[Int]) = {
            if(a.size == 0){
                b
            }
            else{
                val sum:Int = a.sum
                if(b == None){
                    Some(sum)
                }
                else{
                    val oldSum = b.asInstanceOf[Some[Int]]
                    Some(sum + oldSum.get )
                }
            }
        }
        val result = pairs.updateStateByKey(f1)
        result.print()
        //开始计算
        ssc.start()
        //等待结束
        ssc.awaitTermination()
        //停止流计算
        ssc.stop()
    }
}
