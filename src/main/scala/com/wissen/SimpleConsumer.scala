package com.wissen

import com.wissen.moketypes.Transaction
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.concurrent.duration._

object SimpleConsumer {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder().master("yarn").appName("Amex Consumer").getOrCreate()
    val ssc = new StreamingContext(ss.sparkContext, Seconds(180))
    import ss.implicits._
    import io.delta.implicits._

    val schema = ScalaReflection.schemaFor[Transaction].dataType.asInstanceOf[StructType]
    val sq = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka2.datacanny.com:9092,kafka3.datacanny.com:9092")
      .option("subscribe", "amex")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("tmp"))
      .select("tmp.*")

      sq.writeStream
      .format("delta")
      .option("checkpointLocation", "/amex/checkpoints/transactions_simpleconsumer")
      .start("/amex/data/raw/transactions_simpleconsumert")


//    sq.awaitTermination()
//    sq.stop()

  }

}
