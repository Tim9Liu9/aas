import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
  * 标签生成器
  */
object SparkStreamingUpdateStateByKeyScala {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("SparkStreamingDemo")
        conf.setMaster("local[4]")

        def createContext = {
            new StreamingContext(conf,Seconds(1))
        }

        //实现driver程序的故障恢复，通过检查点目录
        val chkdir = "file:///d:/spark/chkdir"
        val ssc = StreamingContext.getOrCreate(chkdir, createContext _)

        //val ssc = new StreamingContext(conf, Seconds(5));
        ssc.checkpoint(chkdir)
        val lines = ssc.socketTextStream("localhost", 8888);
        val words = lines.flatMap(_.split(" "))
        val pairs = words.map((_,1))

        //定义函数
        def f1(a: Seq[Int], b: Option[Int]) = {
            if (a.size == 0) {
                b
            }
            else {
                val asum: Int = a.sum
                if(asum >= 10){
                    None
                }
                else{
                    if(b == None){
                       Some(asum)
                    }
                    else{
                        val rand = new Random()
                        if (rand.nextBoolean()) {
                            //println("出错了")
                            //throw new NullPointerException("Error")}
                        };
                        val oldSum = b.asInstanceOf[Some[Int]]
                        val newSum = asum + oldSum.get
                        if (newSum >= 10) None
                        else Some(newSum)
                    }
                }
            }
        }
        //该方法必须结合checkpoint使用。
        val result = pairs.updateStateByKey(f1)
        result.saveAsTextFiles("file:///d:/spark/result")

        ssc.start();
        ssc.awaitTermination();
    }
}
