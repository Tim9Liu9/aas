import com.alibaba.fastjson.{JSON, JSONPObject}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 标签生成器
  */
object TagGenerator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("wordCountScala")
        conf.setMaster("local[4]")
        val sc = new SparkContext(conf)
        //加载文件
        val rdd1 = sc.textFile("file:///d:/spark/temptags.txt")
        //变换
        val rdd2 = rdd1.map(line=>{
            val arr = line.split("\t");
            val busId = arr(0)
            val txt = arr(1)
            val jo = JSON.parseObject(txt)
            val jarr = jo.getJSONArray("extInfoList")
            if(jarr != null && jarr.size() >0){
                val v1 = jarr.getJSONObject(0);
                val arr2 = v1.getJSONArray("values")
                if( arr2 != null && arr2.size() >0){
                    var str = "" ;
                    var i = 0
                    while(i < arr2.size()){
                        str = str + arr2.getString(i) + ","
                        i += 1
                    }
                    (busId,str.substring(0,str.length - 1)) ;
                }
                else (busId, "")
            }
            else (busId,"") ;
        })
        //过滤,没有评论的滤掉
        val rdd3 = rdd2.filter(t=>{
            t._2 != null && !"".equals(t._2)
        })
        //按照value压扁
        var rdd4 = rdd3.flatMapValues(_.split(","))
        //重组key busId-comm,1
        val rdd5 = rdd4.map(t=>{
            (t._1 + "-" + t._2 , 1)
        })
        //聚合
        val rdd6 = rdd5.reduceByKey(_ + _)

        //变换成(busId,(comm,count))
        val rdd7 = rdd6.map(t=>{
            var arr = t._1.split("-")
            (arr(0),(arr(1),t._2) :: Nil)
        })
        //按照busId进行聚合，value是List
        val rdd8 = rdd7.reduceByKey(_ ++ _)
        //按照key排序(倒序)
        val rdd9 = rdd8.map(t => {
            val x = t._2.sortBy(t=>{
                t._2
            }).reverse.take(5)
            (t._1,x)
        })

        val rdd99 = rdd9.sortBy(t=>{
            t._2(0)._2
        },false,1)

        //变换成(busId,)
        val rdd10 = rdd99.map(t=>{
            val col = t._2
            var desc = "" ;
            for (tt <- col){
                desc = desc + tt._1 + "(" + tt._2+"),"
            }
            (t._1,desc)
        })
        rdd10.foreach(println)
    }
}
