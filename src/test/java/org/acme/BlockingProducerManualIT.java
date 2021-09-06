//package org.acme;
//
//import static org.junit.jupiter.api.TestInstance.Lifecycle;
//
//import java.util.Map;
//import java.util.Properties;
//import java.util.logging.Logger;
//
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.Producer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.junit.jupiter.api.AfterAll;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.Disabled;
//import org.junit.jupiter.api.Tag;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.TestInstance;
//
//@Tag("QUARKUS-1090")
//@TestInstance(Lifecycle.PER_CLASS)
//public class BlockingProducerManualIT {
//
//    private static final Logger LOG = Logger.getLogger(BlockingProducerManualIT.class.getSimpleName());
//    CustomStrimziKafkaContainer kafkaContainer;
//
//    @BeforeAll
//    public void beforeAll() {
//        Map<String, String> kafkaProp = Map.of("auto.create.topics.enable", "false");
//        kafkaContainer = new CustomStrimziKafkaContainer("0.24.0-kafka-2.7.0", kafkaProp);
//        kafkaContainer.start();
//    }
//
//    @AfterAll
//    public void afterAll() {
//        kafkaContainer.stop();
//    }
//
//
//    @Test
//    @Disabled
//    public void kafkaClientsBlocksIfTopicsNotExist() {
//        Properties properties = new Properties();
//        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
//        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, BlockingProducerManualIT.class.getSimpleName());
//        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000"); // Default is 60 seconds
//
//        try (Producer<String, String> producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer())) {
//            System.out.println("Sending message");
//            producer.send(new ProducerRecord<>("ThisTopicDoesNotExist", "key", "value"), (metadata, exception) -> {
//                if (exception != null) {
//                    System.err.println("Send failed: " + exception);
//                } else {
//                    System.out.println(String.format("Send successful: %s-%s/%s", metadata.topic(), metadata.partition(), metadata.offset()));
//                }
//            });
//
//            LOG.info("Sending message");
//            producer.send(new ProducerRecord<>("ThisTopicDoesNotExistToo", "key", "value"), (metadata, exception) -> {
//                if (exception != null) {
//                    System.err.println("Send failed: " + exception);
//                } else {
//                    System.out.println(String.format("Send successful: %s-%s/%s", metadata.topic(), metadata.partition(), metadata.offset()));
//                }
//            });
//        }
//    }
//}
