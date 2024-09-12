package psam1017.study.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Ex4_ExactPartitionConsumer {

    private static final Logger logger = LoggerFactory.getLogger(Ex4_ExactPartitionConsumer.class);
    private static final String TOPIC_NAME = "hello-kafka";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "hello-kafka-group";
    private static Consumer<String, String> consumer;

    public static void main(String[] args) {

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // 컨슈머를 명시적으로 종료함을 알린다.
            // wakeup 메소드를 호출한 이후에 poll 메소드를 호출하면 WakeupException 이 발생한다.
            consumer.wakeup();
            logger.info("Shutdown hook");
        }));

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        int count = 0;

        consumer = new KafkaConsumer<>(properties);
        try {
            consumer.subscribe(List.of(TOPIC_NAME));

            while (count < 10) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("=============================================");
                    logger.info("");
                    logger.info("Message received: {}", record);
                    logger.info("");
                    logger.info("=============================================");
                }
                count++;
            }
        } catch (WakeupException e) {
            // WakeupException 을 받은 후 최종적으로 consumer 를 종료(close)함으로 안전하게 자원이 해제됨을 보장할 수 있다.
            logger.warn("Wakeup consumer");
        } finally {
            consumer.close();
        }
    }
}
