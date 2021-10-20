package com.example.spark_cloud;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.spark_cloud.sparks.bean.ConfigurationManager;
import com.example.spark_cloud.sparks.bean.Lines;
import com.example.spark_cloud.utils.Cal_sta;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

public class SparkSteamingKafka {
    public static List<Lines> lines;
    public static List<Map<String, Object>> maps;

    public SparkSteamingKafka() {
    }

    public static void main(String[] args) {
        SparkConf conf = (new SparkConf()).setAppName("SparkSteamingKafka").setMaster("local[2]");
        lines = new ArrayList();
        maps = new ArrayList();
        conf.set("es.nodes", ConfigurationManager.getProperty("es.ip"));
        conf.set("es.index.auto.create", "true");
        conf.set("es.port", ConfigurationManager.getProperty("es.port"));
        conf.set("es.nodes.wan.only", "true");
        conf.set("es.net.http.auth.user", "elastic");
        conf.set("es.net.http.auth.pass", "igy294Cb45kqquqatLve");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        sc.setCheckpointDir("./checkpoint");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sc, Durations.seconds(50));
        Map<String, Object> kafkaParams = new HashMap();
        kafkaParams.put("bootstrap.servers", ConfigurationManager.getProperty("bootstrap.servers"));
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", ConfigurationManager.getProperty("group.id"));
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);
        String kafkaTopics = ConfigurationManager.getProperty("kafka.topics");
        String[] kafkaTopicsSplited = kafkaTopics.split(",");
        Collection<String> topics = new HashSet();
        String[] var8 = kafkaTopicsSplited;
        int var9 = kafkaTopicsSplited.length;

        for(int var10 = 0; var10 < var9; ++var10) {
            String kafkaTopic = var8[var10];
            topics.add(kafkaTopic);
        }

        try {
            JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));
            JavaPairDStream<Object, Iterable<String>> words = stream.mapToPair((record) -> {
                return new Tuple2(JSON.parseObject((String)record.value()).get("name"), record.value());
            }).groupByKey();
            words.foreachRDD(new VoidFunction<JavaPairRDD<Object, Iterable<String>>>() {
                @Override
                public void call(JavaPairRDD<Object, Iterable<String>> param) throws Exception {
                    param.foreach((t) -> {
                        HashMap<String, Object> map = new HashMap();
                        map.put("high_rate_line", "长春-" + (String)((Entry)((List)((TreeMap)JSON.parseArray(String.valueOf(t._2)).stream().collect(Collectors.groupingBy((obj) -> {
                            return ((JSONObject)obj).getString("end_location");
                        }, TreeMap::new, Collectors.counting()))).entrySet().stream().sorted(Comparator.comparingLong((obj) -> {
                            return (Long)((Entry)obj).getValue();
                        }).reversed()).collect(Collectors.toList())).get(0)).getKey());
                        BigDecimal b = new BigDecimal(JSON.parseArray(String.valueOf(t._2)).stream().mapToDouble((obj) -> {
                            return ((JSONObject)obj).getDouble("score");
                        }).average().getAsDouble());
                        double f1 = b.setScale(2, 4).doubleValue();
                        Cal_sta cal = new Cal_sta();
                        List<Double> lists = (List)JSON.parseArray(String.valueOf(t._2)).stream().map((obj) -> {
                            return ((JSONObject)obj).getDouble("price");
                        }).collect(Collectors.toList());
                        Double[] scales = (Double[])((Double[])lists.toArray(new Double[lists.size()]));
                        double avg = JSON.parseArray(String.valueOf(t._2)).stream().mapToDouble((obj) -> {
                            return ((JSONObject)obj).getDouble("price");
                        }).average().getAsDouble();
                        double num = cal.Sample_STD_dev(scales);
                        double proportion = num / avg;
                        BigDecimal c = new BigDecimal(proportion);
                        double f2 = c.setScale(4, 4).doubleValue() * 100.0D;
                        BigDecimal d = new BigDecimal(f1 + f2);
                        double f3 = d.setScale(2, 4).doubleValue();
                        map.put("price_variance", f2);
                        map.put("score_average", f1);
                        map.put("marks", f3);
                        map.put("line_name", t._1.toString());
                        Date d1 = new Date();
                        Calendar cal1 = Calendar.getInstance();
                        cal1.setTime(d1);
                        cal1.add(Calendar.MINUTE, 10);
                        d1 = cal1.getTime();
                        SimpleDateFormat sbf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        map.put("time", sbf.format(d1));
                        SparkSteamingKafka.maps.add(map);
                    });
                    JavaRDD<Map<String, Object>> venderRDD = sc.parallelize(SparkSteamingKafka.maps);
                    JavaEsSpark.saveToEs(venderRDD, "calculation");
                }
            });
            words.print();
            streamingContext.start();
            streamingContext.awaitTermination();
        } catch (Exception var12) {
            var12.printStackTrace();
        }

    }
}
