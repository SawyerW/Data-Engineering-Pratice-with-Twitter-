package com.sqlknowledgebank.spark.sparkstreaming
import java.util.Properties
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import twitter4j.Status
import twitter4j._
import twitter4j.conf._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
object twitter {

  def main(args: Array[String]) {

// settings for kafka connection
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9091")
    props.put("acks", "all")
    props.put("retries", 0)
    props.put("batch.size", 16384)
    props.put("linger.ms", 1)
    props.put("buffer.memory", 33554432)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val kafkaProducer = new KafkaProducer[String, String](props)
    
    // below four variables are api keys and secrect you need to get from twitter 
    val consumerKey = "r0c"
    val consumerSecretKey = "RKZFp7P"
    val accessToken = "24xgcY"
    val accessTokenSecret = "SZRCgR"


    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecretKey)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)



// get twitter data and push data to kafka
      val twitterStream = new TwitterStreamFactory(cb.build).getInstance()

      val listener = new StatusListener() {
      def onStatus(status: Status) {
        if (!status.isRetweet()){println(status.getId,status.getText);
          kafkaProducer.send(new ProducerRecord("Test", status.getId.toString,status.getText))}
        }
      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
      def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
      def onException(ex: Exception) { ex.printStackTrace }
      def onScrubGeo(arg0: Long, arg1: Long) {}
      def onStallWarning(warning: StallWarning) {}
    }


    twitterStream.addListener(listener)
// add a filter
    val query = new FilterQuery().track("China")
    twitterStream.filter(query)
//    Thread.sleep(5000)


//    println(twitterStream)


//    val tweets = TwitterUtils.createStream(ssc, Some(auth))
//
////    tweets .saveAsTextFiles("tweets", "json")
//
//    println(tweets)
//    ssc.start()
//    ssc.awaitTermination()
  }


}
