package com.wissen

import com.wissen.moketypes.Transaction
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AmexConsumer {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("yarn").appName("Amex Consumer").getOrCreate()
    val ssc = new StreamingContext(ss.sparkContext, Seconds(300))

    import ss.implicits._

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka2.datacanny.com:9092,kafka3.datacanny.com:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("amex")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => (record.key, record.value)).foreachRDD(rdd => {

      rdd
        .map(record => Transaction.fromJson(record._2))
        .toDS()
        .write
        .mode("append")
        .format("parquet")
        .save("/amex/data/raw/transactions_amexconsumer")

    })


    ssc.checkpoint("/amex/checkpoints/transactions_amexconsumer_parquet")
    ssc.start()
    ssc.awaitTermination()

  }
}
