package com.flipkart;


import com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SparkConsumer {

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf()
                .setAppName("kafka-sandbox")
                .setMaster("local[*]") // Set your Spark master here
                .set("es.nodes", "0.0.0.0") // Set your Elasticsearch nodes
                .set("es.port", "9200") // Set your Elasticsearch port
                .set("es.index.auto.create", "true")
                .set("es.nodes.wan.only", "true");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(5000)); // Batch interval

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092"); // Set your Kafka broker(s)
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark-consumer-group"); // Set your consumer group
        kafkaParams.put("enable.auto.commit", true);

        Set<String> topics = Collections.singleton("travel"); // Set your Kafka topic(s)

        JavaInputDStream<ConsumerRecord<Object, Object>> kafkaStream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        kafkaStream.foreachRDD(rdd -> {
            rdd.foreach(record -> {
                System.out.printf(
                        "Received message: topic = %s, partition = %d, offset = %d, key = %s, value = %s%n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value()
                );
//                String val = (String) record.value();
//                JavaEsSpark.saveToEs(val, "travel");
            });

            if (!rdd.isEmpty()) {
                System.out.println("Writing to ES");
                JavaEsSpark.saveToEs(rdd.map(ConsumerRecord::value), "travel"); // Set your Elasticsearch index and type
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}

