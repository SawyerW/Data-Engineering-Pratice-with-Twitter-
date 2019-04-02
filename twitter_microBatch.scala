import org.apache.spark.SparkConf
import org.apache.spark._
import java.sql.Timestamp
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.streaming.Trigger

object twitter_microBatch {




  val check_point = "/opt/kafka/check_point_processed/"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("microbatch")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9091")
      .option("subscribe", "Test")
//      .option("startingOffsets", "earliest")
//      .option("endingOffsets", "latest")
      .load()
    val tt = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)",  "CAST(timestamp AS timestamp)")
//      .alias("value")
      .as[(String, String, Timestamp)]
    df.printSchema()
    tt.printSchema()



    import org.apache.spark.sql.expressions.scalalang.typed
    val aggregates = tt
      .withWatermark("timestamp", "3 seconds") //set watermark using timestamp filed with a max delay of 3s.

      .groupBy(window($"timestamp","4 seconds","2 seconds"), $"value").count()
//      .select("value","key", "count(key)","window")


    aggregates.printSchema()





    aggregates
      .selectExpr("CAST(window AS struct<start:timestamp,end:timestamp>)",  "CAST(count AS string)  as count", "CAST (value as STRING)")
//      .selectExpr("window",  "CAST(count(key) AS string)  as count", "CAST (key as STRING)")
      .writeStream

      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9091")
      .option("checkpointLocation", check_point)
      .option("topic", "backtokafka")
      .start()


    spark.streams.awaitAnyTermination()
  }



}
