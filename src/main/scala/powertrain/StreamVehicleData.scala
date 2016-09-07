package powertrain

/**
  * Created by sebastianestevez on 6/1/16.
  */

import java.sql.Timestamp

import com.datastax.driver.dse.graph.{GraphNode, GraphOptions, GraphResultSet, SimpleGraphStatement}
import com.datastax.driver.dse.{DseCluster, DseSession}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture, MoreExecutors}
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object StreamVehicleData {

  def check(x: Int) = if (x == 1) "Peyton" else "Ryan"

  val localLogger = Logger.getLogger("StreamVehicleData")

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    val debug = sparkConf.get("spark.debugging", "false").toBoolean
    val graph_name = sparkConf.get("spark.graph_name")
    val dse_host = sparkConf.get("spark.dse_host")


    if (debug) {
      localLogger.info("WARNING!!! Running in local debug mode!  Initializing graph schema")

      /*
         with dse spark submit these are all set for you.  Do they need to be set for local development?
         sparkConf
         .setMaster("local[1]")
         .setAppName(graph_name)
         .set("spark.cassandra.connection.host", "dse_host")
        */

      // Creates the graph if it does not exist
      initialize_graph(dse_host, graph_name)
      // Drops the schema and recreates it
      val session = get_dse_session(dse_host, graph_name)
      initialize_schema(session, "schema")
      session.close()
    }


    val contextDebugStr: String = sparkConf.toDebugString
    localLogger.info("contextDebugStr = " + contextDebugStr)

    def createStreamingContext(): StreamingContext = {
      @transient val newSsc = new StreamingContext(sparkConf, Seconds(1))
      localLogger.info(s"Creating new StreamingContext $newSsc")
      newSsc
    }

    val sparkStreamingContext = StreamingContext.getActiveOrCreate(createStreamingContext)

    val sc = SparkContext.getOrCreate(sparkConf)
    val sqlContext = SQLContext.getOrCreate(sc)

    //not checkpointing
    //ssc.checkpoint("/ratingsCP")

    val topicsArg = "vehicle_events"
    val brokers = sparkConf.get("spark.kafka_brokers", "localhost:9092")
    val debugOutput = true


    val topics: Set[String] = topicsArg.split(",").map(_.trim).toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    localLogger.info(s"connecting to brokers: $brokers")
    localLogger.info(s"sparkStreamingContext: $sparkStreamingContext")
    localLogger.info(s"kafkaParams: $kafkaParams")
    localLogger.info(s"topics: $topics")


    import com.datastax.spark.connector.streaming._

    val rawVehicleStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](sparkStreamingContext, kafkaParams, topics)
    rawVehicleStream.print()

    val splitArray = rawVehicleStream.map { case (key, rawVehicleStr) =>
      val strings = rawVehicleStr.split(",")

      val logger = Logger.getLogger("StreamVehicleData")
      logger.info(s"update type: ${strings(0)}")
      strings
    }

    splitArray.filter(data => data(0) == "location")
      .map { data =>
        val logger = Logger.getLogger("StreamVehicleData")
        logger.info(s"vehicle location: ${data(1)}")
        VehicleLocation(vehicle_id = data(1).toLowerCase, lat_long = data(2), elevation = data(3), speed = data(4).toDouble, acceleration = data(5).toDouble, time_period = new Timestamp(data(6).toLong), collect_time = new Timestamp(data(7).toLong), tile2 = data(8), elapsed_time = data(9).toInt)
      }
      .saveToCassandra("vehicle_tracking_app", "vehicle_stats")


    val vehicleEventsStream: DStream[VehicleEvent] = splitArray.filter(data => data(0) == "event").map { data =>
      VehicleEvent(vehicle_id = data(1).toLowerCase, event_name = data(2), event_value = data(3), time_period = new Timestamp(data(4).toLong), collect_time = new Timestamp(data(5).toLong), elapsed_time = data(6).toInt)
    }

    vehicleEventsStream
      .saveToCassandra("vehicle_tracking_app", "vehicle_events")


    vehicleEventsStream.foreachRDD(event_partitions => {
      event_partitions.foreachPartition(events => {



        if (events.nonEmpty) {
          processVehicleEventsStream(events, dse_host, graph_name)
        }

      })
    })

    //Kick off
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }

  def processVehicleEventsStream(events: Iterator[VehicleEvent], dse_host:String, graph_name:String) = {

    val create_event = new SimpleGraphStatement(
      """
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
    val user_exists = new SimpleGraphStatement(
      """
            g.V().has('github_user','account', account)
      """)

    val logger = Logger.getLogger("StreamVehicleData")
    val session = get_dse_session(dse_host, graph_name)

    events.foreach(vehicleEvent => {
      if (vehicleEvent.event_name == "crash" || vehicleEvent.event_name == "lap" || vehicleEvent.event_name == "finish") {
        val userFuture = session.executeGraphAsync(user_exists.set("account", vehicleEvent.vehicle_id))

        Futures.addCallback(userFuture, new FutureCallback[GraphResultSet]() {
          def onSuccess(graphResultSet: GraphResultSet) {
            if (graphResultSet.getAvailableWithoutFetching > 0) {
              val user = graphResultSet.one()
              processUser(session, create_event, create_event_edge, logger, vehicleEvent, user)
            }
            else {
              logger.info(s"Error query user_exists --> result set was empty")
            }
          }

          def onFailure(thrown: Throwable) {
            logger.info(s"Error running graph query user_exists ${user_exists.getQueryString}")
            logger.info(s"Error running graph query user_exists $thrown")
          }
        })
      }
    })
    session.close()
  }


  def processUser(session: DseSession, create_event: SimpleGraphStatement, create_event_edge: SimpleGraphStatement, logger: Logger, vehicleEvent: VehicleEvent, user: GraphNode): Unit = {
    if (user != null) {
      create_event
        .set("vehicle_id", vehicleEvent.vehicle_id.toString)
        .set("time_period", vehicleEvent.time_period.toString)
        .set("collect_time", vehicleEvent.collect_time.toString)
        .set("event_name", vehicleEvent.event_name.toString)
        .set("event_value", vehicleEvent.event_value.toString)
        .set("elapsed_time", vehicleEvent.elapsed_time.toString)

      logger.info(s"create_event query: ${create_event.getQueryString}")

      val lapEventFuture: ListenableFuture[GraphResultSet] = session.executeGraphAsync(create_event)

      Futures.addCallback(lapEventFuture, new FutureCallback[GraphResultSet]() {
        def onSuccess(graphResultSet: GraphResultSet) {
          if (graphResultSet.getAvailableWithoutFetching > 0) {
            processLapEventFuture(session, create_event_edge, logger, vehicleEvent, graphResultSet)
          }
          else {
            logger.info(s"Error graphResultSet was empty")
          }
        }

        def onFailure(thrown: Throwable) {
          logger.info(s"Error running graph query create_event ${create_event.getQueryString}")
          logger.info(s"Error running graph query create_event ${vehicleEvent.vehicle_id}")
          logger.info(s"Error running graph query create_event ${vehicleEvent.time_period}")
          logger.info(s"Error running graph query create_event ${vehicleEvent.collect_time}")
          logger.info(s"Error running graph query create_event ${vehicleEvent.event_name}")
          logger.info(s"Error running graph query create_event ${vehicleEvent.event_value}")
          logger.info(s"Error running graph query create_event ${vehicleEvent.vehicle_id}")

        }
      })

    }
  }

  def processLapEventFuture(session: DseSession, create_event_edge: SimpleGraphStatement, logger: Logger,
                            vehicleEvent: VehicleEvent, graphResultSet: GraphResultSet): Unit = {

    val vertexId = graphResultSet.one().asVertex().getId
    logger.info(s"vertexId: $vertexId")

    create_event_edge
      .set("event_id", vertexId)
      .set("account", vehicleEvent.vehicle_id)

    logger.info(s"running create_event_edge: ${create_event_edge.getQueryString}")

    val runnable = createRunnable { () =>
      try
        logger.info(s"finished create_event_edge: ${create_event_edge.getQueryString}")
      catch {
        case e: Exception =>
          logger.info(s"Error create_event_edge: ${create_event_edge.getQueryString}")
      }
    }

    session.executeGraphAsync(create_event_edge).addListener(runnable, MoreExecutors.sameThreadExecutor)
  }

  def createRunnable(f: () => Unit): Runnable =
    new Runnable() {
      def run() = f()
    }

  def get_dse_session(dse_host: String, graph_name: String): DseSession = {
    val dseCluster = if (graph_name ne "")
      new DseCluster.Builder().addContactPoint(dse_host).withGraphOptions(new GraphOptions().setGraphName(graph_name)).build
    else
      new DseCluster.Builder().addContactPoint(dse_host).build

    dseCluster.connect()
  }

  def initialize_schema(dseSession: DseSession, schema_file: String): Boolean = {
    dseSession.executeGraph("schema.clear()")
    val schema = scala.io.Source.fromFile(getClass().getResource(schema_file).getFile()).getLines() foreach (line => {
      dseSession.executeGraph(line)
    })
    true
  }

  def initialize_graph(dse_host: String, graph_name: String): Boolean = {
    val dseSession = get_dse_session(dse_host, graph_name)
    dseSession.executeGraph(new SimpleGraphStatement(
      "system.graph(graph_name).ifNotExists().create()"
    ).set("graph_name", graph_name))

    true
  }
}
