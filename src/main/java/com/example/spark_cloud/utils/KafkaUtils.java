package com.example.spark_cloud.utils;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaUtils {

    static final String servers = "10.133.69.110:9092";

    public KafkaUtils() {
    }

    public static KafkaConsumer<String, String> createConsumer(String servers, String topic) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", servers);
        properties.put("group.id", "spark-group-10");
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));
        return kafkaConsumer;
    }

    public static void readMessage(KafkaConsumer<String, String> kafkaConsumer, int timeout) {
        while(true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis((long)timeout));
            Iterator var3 = records.iterator();

            while(var3.hasNext()) {
                ConsumerRecord<String, String> record = (ConsumerRecord)var3.next();
                String value = (String)record.value();
                kafkaConsumer.commitAsync();
                System.out.println(value);
            }
        }
    }

    public static KafkaProducer<String, String> createProducer(String servers) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", servers);
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer(properties);
    }

    public static void send(KafkaProducer<String, String> producer, String topic, String message) {
        producer.send(new ProducerRecord(topic, message), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                Logger logger = LogManager.getLogger("MyFlink");
                if (exception != null) {
                    logger.error("发送消息异常：" + exception);
                } else {
                    logger.info("发送数据:" + message);
                }

            }
        });
    }

    public static AdminClient adminClient() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.133.69.110:9092");
        return AdminClient.create(properties);
    }

    public static void createTopic(String name) throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        int numPartitions = 1;
        short replicationFactor = 1;
        NewTopic topic = new NewTopic(name, numPartitions, replicationFactor);
        CreateTopicsResult result = adminClient.createTopics(Arrays.asList(topic));
        Thread.sleep(500L);
        System.out.println(result.numPartitions(name).get());
    }

    public static void topicLists() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        ListTopicsResult result1 = adminClient.listTopics();
        System.out.println(result1.names().get());
        System.out.println(result1.listings().get());
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        ListTopicsResult result2 = adminClient.listTopics(options);
        System.out.println(result2.names().get());
    }

    public static void delTopics(String topicName) throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        DeleteTopicsResult result = adminClient.deleteTopics(Arrays.asList(topicName));
        System.out.println(result.all().get());
    }

    public static void describeTopics(String topicName) throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        DescribeTopicsResult result = adminClient.describeTopics(Arrays.asList(topicName));
        Map<String, TopicDescription> descriptionMap = (Map)result.all().get();
        descriptionMap.forEach((key, value) -> {
            System.out.println("name: " + key + ", desc: " + value);
        });
    }

    public static void describeConfig(String topicName) throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        ConfigResource configResource = new ConfigResource(Type.TOPIC, topicName);
        DescribeConfigsResult result = adminClient.describeConfigs(Arrays.asList(configResource));
        Map<ConfigResource, Config> map = (Map)result.all().get();
        map.forEach((key, value) -> {
            System.out.println("name: " + key.name() + ", desc: " + value);
        });
    }

    public static void incrementalAlterConfig(String topicName) throws Exception {
        ConfigResource configResource = new ConfigResource(Type.TOPIC, topicName);
        Collection<AlterConfigOp> configs = Arrays.asList(new AlterConfigOp(new ConfigEntry("preallocate", "false"), OpType.SET));
        AdminClient adminClient = adminClient();
        Map<ConfigResource, Collection<AlterConfigOp>> configMaps = new HashMap();
        configMaps.put(configResource, configs);
        AlterConfigsResult result = adminClient.incrementalAlterConfigs(configMaps);
        System.out.println(result.all().get());
    }

    public static void main(String[] args) {
        try {
           // createTopic("supplier_car_p");
            KafkaConsumer<String, String> consumer = createConsumer(servers, "supplier_car_ss");

            topicLists();
            incrementalAlterConfig("supplier_car_p");
        } catch (Exception var2) {
            var2.printStackTrace();
        }

    }

}
