import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object spark_self_provided_receiving_package {
  def main(args: Array[String]): Unit = {

//    if (args.length < 4) {
//      System.err.println("Usage: KafkaWordCount <zkQuorum><group> <topics> <numThreads>")
//      System.exit(1)
//    }
    val check_point = "/opt/kafka/check_point/"

    val conf = new SparkConf().setMaster("local[2]").setAppName("kafka_test")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint(check_point)


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9091",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "second",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
//    val offsets = Map(new TopicPartition("topicB", 0) -> 2L)
//    val topics = Array("topicA", "topicB")
    val topicName = "Test"
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Set(topicName), kafkaParams)
    )

//    stream.map(record => (record.key, record.value))

    stream.map(_.value)
      .flatMap(_.split(" "))
      .map(x => (x, 1L))
      .reduceByKey(_ + _)
      .transform(data => {
        val sortData = data.sortBy(_._2, false)
        sortData
      })
      .print()

    ssc.start()
    ssc.awaitTermination()

//    stream.foreachRDD { rdd =>
//      // Get the offset ranges in the RDD
//      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      for (o <- offsetRanges) {
//        println(s"${o.topic} ${o.partition} offsets: ${o.fromOffset} to ${o.untilOffset}")
//      }
//    }
//
//
//    ssc.start()

    // the above code is printing out topic details every 5 seconds
    // until you stop it.

//    ssc.stop(stopSparkContext = false)
  }

}
