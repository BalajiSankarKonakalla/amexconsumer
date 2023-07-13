package com.wissen

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import com.wissen.moketypes.Transaction
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel



object ReadEvents {

  def createStreamingContext(): StreamingContext = {

    val ss = SparkSession.builder().master("yarn").appName("Amex Consumer").getOrCreate()
    val ssc = new StreamingContext(ss.sparkContext, Seconds(180))

    val kafkaParams = getKafkaConf
    val topics = Array("amex")

    val amexKafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    import ss.implicits._

    amexKafkaStream
      .map(record => (record.key, record.value)).persist(StorageLevel.MEMORY_ONLY_SER)
      .repartition(3)
        .foreachRDD(rdd => {
          rdd.map(t => {
            Transaction.fromJson(t._2)
          }).coalesce(1)
            .toDS
            .write.mode("append")
            .format("delta")
            .save("/amex/data/raw/transactions-readevents-delta")
        })
    ssc.checkpoint("/amex/checkpoints/amex-consume-readevents-delta")
    ssc
  }

  def getKafkaConf: Map[String, Object] = {
    Map[String, Object](
      "bootstrap.servers" -> "kafka2.datacanny.com:9092,kafka3.datacanny.com:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
  }

  def main(args: Array[String]): Unit = {

    val ssc = StreamingContext.getOrCreate("/amex/checkpoints/amex-consumer-readevents-delta", createStreamingContext)

    ssc.start()
    ssc.awaitTermination()
  }

}
