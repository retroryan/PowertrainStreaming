/**
 * Created by sebastianestevez on 6/1/16.
 */

import java.sql.Timestamp

import com.datastax.driver.dse.DseSession
import com.datastax.driver.dse.graph.SimpleGraphStatement
import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object StreamVehicleData {
  def main(args: Array[String]) {

    val configValues = initialize()
    initialize_graph(configValues.get("dse_host").get, configValues.get("graph_name").get)


    val session = get_dse_session(configValues.get("dse_host").get, configValues.get("graph_name").get)
    initialize_schema(session, "schema")
    session.close()

    val sparkConf = new SparkConf()

//  Added for local debugging in IntelliJ
        .setMaster("local[1]")
        .setAppName(configValues.get("graph_name").get)
        .set("spark.cassandra.connection.host", configValues.get("dse_host").get)

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
        VehicleEvent(data(1), data(2), data(3), new Timestamp(data(4).toLong), new Timestamp(data(5).toLong), data(6).toInt)
      }
      .saveToCassandra("vehicle_tracking_app", "vehicle_events")
    splitArray.filter(data => data(0) == "event").foreachRDD( event_partitions => {
      event_partitions.foreachPartition(events => {
        if(!events.isEmpty) {
          val session = get_dse_session(configValues.get("dse_host").get, configValues.get("graph_name").get)
          val create_event = new SimpleGraphStatement("""
            graph.addVertex(label, 'powertrain_events',
                            'vehicle_id', vehicle_id,
                            'time_period', time_period,
                            'collect_time', collect_time,
                            'event_name', event_name,
                            'event_value', event_value,
                            'elapsed_time', elapsed_time)
          """)
          val create_event_edge = new SimpleGraphStatement(
              "def event = g.V(event_id).next()\n" +
              "def user = g.V().hasLabel('github_user').has('account', account).next()\n" +
              "user.addEdge('has_events', event)"
          )

          events.foreach(data => {
            if(data(2) == "lap" || data(2) == "finish") {
              create_event
                .set("vehicle_id", data(1))
                .set("time_period", data(4))
                .set("collect_time", data(5))
                .set("event_name", data(2))
                .set("event_value", data(3))
                .set("elapsed_time", data(6).toInt)
              val lap_event = session.executeGraph(create_event)

              create_event_edge
                .set("event_id", lap_event.one().asVertex().getId)
                .set("account", data(1))

              session.executeGraph(create_event_edge)
            }
          })


      })
    })
    //Kick off
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }
  def get_dse_session(dse_host: String, graph_name: String): DseSession = {
    val dseCluster = new DseJavaDriverWrapper().CreateNewCluster(dse_host, graph_name)
    return dseCluster.connect()
  }
  // Initialization
  def initialize(): Map[String, String] = {
    // Load Configuration Properties
    val appConfig = ConfigFactory.load();

    return Map(
      "graph_name" -> appConfig.getString("appConfig.graph_name"),
      "dse_host" -> appConfig.getString("appConfig.dse_host"),
      "spark_master" -> appConfig.getString("appConfig.spark_master"),
      "spark_name" -> appConfig.getString("appConfig.spark_name")
    )
  }
  def initialize_schema(dseSession: DseSession, schema_file: String): Boolean ={
    val schema = scala.io.Source.fromFile(getClass().getResource(schema_file).getFile()).getLines()foreach(line => {
      dseSession.executeGraph(line)
    })
    return true
  }
  def initialize_graph(dse_host: String, graph_name: String): Boolean ={
    val dseCluster = new DseJavaDriverWrapper().CreateNewCluster(dse_host, "")
    val dseSession = dseCluster.connect()
    dseSession.executeGraph(new SimpleGraphStatement(
      "system.graph(graph_name).ifNotExists().create()"
    )
      .set("graph_name", graph_name))

    return true
  }
}
