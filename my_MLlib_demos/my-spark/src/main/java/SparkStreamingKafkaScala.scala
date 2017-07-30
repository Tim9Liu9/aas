/**
  * Created by Administrator on 2017/7/16.
  */

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object SparkStreamingKafkaScala {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
        conf.setAppName("spark-kafka")
        conf.setMaster("local[4]")

        val ssc = new StreamingContext(conf, Seconds(1))
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "s202:9092,s203:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "g1",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val topics = Array("topic1")
        val stream = KafkaUtils.createDirectStream[String, String](ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )

        stream.flatMap(r=>{r.value().split(" ")}).map((_,1)).reduceByKey( _ + _).print()
        ssc.start()
        ssc.awaitTermination()

    }
}
