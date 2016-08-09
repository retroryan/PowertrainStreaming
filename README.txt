dse spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.4.1 --class streaming.StreamVehicleData target/scala-2.10/streaming-vehicle-app_2.10-1.0-SNAPSHOT.jar 10.0.0.4:9092 ratings true

bin/kafka-console-consumer.sh --zookeeper $KAFKA:2181 --topic vehicle_events --from-beginning
dse spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.4.1 --class streaming.StreamVehicleData target/scala-2.10/streaming-vehicle-app_2.10-1.0-SNAPSHOT.jar