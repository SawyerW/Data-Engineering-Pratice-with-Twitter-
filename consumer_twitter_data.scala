package com.sqlknowledgebank.spark.sparkstreaming
import java.util.Properties
import scala.collection.JavaConversions._
import java.util._
import org.apache.kafka.clients.consumer._
object consumer_twitter_data {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9091")
    props.put("group.id", "kafka_consumer")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer= new KafkaConsumer[String,String](props)



    while (true) {
      consumer.subscribe(Collections.singletonList("backtokafka"))
      val records = consumer.poll(100);
      for (record <- records) {
        println("****************************************")
        println(record)

      }
    }


  }
}
