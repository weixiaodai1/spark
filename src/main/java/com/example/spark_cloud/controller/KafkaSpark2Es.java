package com.example.spark_cloud.controller;

import com.google.gson.Gson;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;

public class KafkaSpark2Es {
    public KafkaSpark2Es() {
    }

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = (new SparkConf()).setMaster("spark://master:7077").setAppName("KafkaSpark2Es").set("spark.executor.memory", "2g").set("spark.dynamicAllocation.enabled", "false");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(10L));
        String brokers = "master:9092";
        String groupId = "kafka-01";
        String topics = "topic1";
        Set<String> topicsSet = new HashSet(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicsSet, kafkaParams));
        JavaDStream<String> lines = messages.map(new Function<ConsumerRecord<String, String>, String>() {
            Map<String, String> map = new HashMap();

            @Override
            public String call(ConsumerRecord<String, String> record) throws Exception {
                String[] splits = ((String)record.value()).split("\\|\\|");
                String[] var3 = splits;
                int var4 = splits.length;

                for(int var5 = 0; var5 < var4; ++var5) {
                    String date = var3[var5];
                    if (date.lastIndexOf("=") != -1 && date.indexOf("=") != date.length()) {
                        this.map.put(date.substring(0, date.indexOf("=")), date.substring(date.indexOf("=") + 1, date.length()));
                    }
                }

                Gson gson = new Gson();
                return gson.toJson(this.map);
            }
        });
        lines.print();
        JavaEsSparkStreaming.saveJsonToEs(lines, "");
        ssc.start();
        ssc.awaitTermination();
    }
}
