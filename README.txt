Spark Streaming

The spark streaming app ingests data off of kafka, processes it in micro batches, and writes to cassandra

Build the streaming app

sbt package
and run from dse spark-submit (DSE must be running with Analytics enabled)

LOCAL RUN
dse spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.1 --conf=spark.cores.max=1  --class powertrain.StreamVehicleData --properties-file=conf/application.conf target/scala-2.10/streaming-vehicle-app_2.10-1.0-SNAPSHOT.jar

SERVER RUN
dse spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.1 --class powertrain.StreamVehicleData  --properties-file=application.conf streaming-vehicle-app_2.10-1.0-SNAPSHOT.jar

SERVER RUN with nohup
nohup dse spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.1 --class powertrain.StreamVehicleData  --properties-file=application.conf streaming-vehicle-app_2.10-1.0-SNAPSHOT.jar 2>&1 1> streaming.log &



-- Watch the kafka queue:

bin/kafka-console-consumer.sh --zookeeper $KAFKA:2181 --topic vehicle_events --from-beginning
dse spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.4.1 --class streaming.StreamVehicleData target/scala-2.10/streaming-vehicle-app_2.10-1.0-SNAPSHOT.jar
