package streaming

/**
 * Created by sebastianestevez on 6/1/16.
 */

import java.sql.Timestamp

import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.{KafkaUtils, KafkaRDDPartition}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object StreamVehicleData {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()

//  Added for local debugging in IntelliJ
//        .setMaster("local[1]")
//        .setAppName("VehicleDataTest")
//        .set("spark.cassandra.connection.host", "127.0.0.1")

    val contextDebugStr: String = sparkConf.toDebugString
    System.out.println("contextDebugStr = " + contextDebugStr)

    def createStreamingContext(): StreamingContext = {
      @transient val newSsc = new StreamingContext(sparkConf, Seconds(1))
      println(s"Creating new StreamingContext $newSsc")
      newSsc
    }

    val sparkStreamingContext = StreamingContext.getActiveOrCreate(createStreamingContext)

    val sc = SparkContext.getOrCreate(sparkConf)
    val sqlContext = SQLContext.getOrCreate(sc)

    //not checkpointing
    //ssc.checkpoint("/ratingsCP")

    //val topicsArg = "vehicle_events,vehicle_status"
    val topicsArg = "vehicle_events"
    val brokers = "localhost:9092"
    val debugOutput = true


    val topics: Set[String] = topicsArg.split(",").map(_.trim).toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    //val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)


    println(s"connecting to brokers: $brokers")
    println(s"sparkStreamingContext: $sparkStreamingContext")
    println(s"kafkaParams: $kafkaParams")
    println(s"topics: $topics")


    import com.datastax.spark.connector.streaming._


    val rawVehicleStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](sparkStreamingContext, kafkaParams, topics)

    val splitArray = rawVehicleStream.map { case (key, rawVehicleStr) =>
      val strings= rawVehicleStr.split(",")

      println(s"update type: ${strings(0)}")
      strings
    }
    splitArray.filter(data => data(0) == "location")
      .map { data =>
        println(s"vehicle location: ${data(1)}")
        VehicleLocation(data(1), data(2), data(3), data(4).toDouble, data(5).toDouble, new Timestamp(data(6).toLong), new Timestamp(data(7).toLong), data(8), data(9).toInt)
      }
      .saveToCassandra("vehicle_tracking_app", "vehicle_stats")

    splitArray.filter(data => data(0) == "event").map { data =>
        println(s"vehicle event: ${data(1)}")
        VehicleEvent(data(1), data(2), data(3), new Timestamp(data(4).toLong), new Timestamp(data(5).toLong), data(6).toInt)
      }
      .saveToCassandra("vehicle_tracking_app", "vehicle_events")

    //Kick off
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }
}
