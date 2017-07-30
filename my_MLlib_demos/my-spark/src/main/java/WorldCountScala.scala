package com.timliu.spark.scala

import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Scala版的wordcount
  */
object WorldCountScala {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("wordCountScala")
        conf.setMaster("local[8,1]")
        //创建SparkContext对象
        val sc = new SparkContext(conf)
        sc.setCheckpointDir("file:///d:/spark")

        val rdd1 = sc.textFile("file:///d:/1.txt", 3)

        //rdd1.persist(StorageLevel.DISK_ONLY)
        case class Dog(val name: String)
        val d = Dog("dahuang");

        //创建累加器
        var acc = sc.longAccumulator("mycount")
        //创建广播变量
        val v = sc.broadcast(d);

        val rdd2 = rdd1.flatMap(e => {
            //            import com.timliu.debugtool.RTUtil
            //            RTUtil.sendInfo("s205",8888,this,"1","")
            println("1 : " + v.value.name)
            acc.add(1)
            e.split(" ")
        })
        //rdd2.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)
        val rdd3 = rdd2.map(e => {
            println("1 : " + v.value.name)
            (e, 1)
        })
        //rdd3.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)
        rdd3.collect()
        println("driver: " + v.value)

        //        val rdd4 = rdd3.reduceByKey((a,b)=>{
        //            println("3");
        //            a + b
        //        })

        //        rdd4.persist(StorageLevel.DISK_ONLY)
        //        rdd4.collect()
        //rdd2.unpersist()
        //rdd3.unpersist()
        rdd3.collect()
        //        rdd4.unpersist()
        //        rdd4.collect()
        //val arr = rdd4.collect()
        //rdd4.collect()
        //arr.foreach(println)
    }
}
