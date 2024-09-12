package psam1017.study.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Ex2_3_RecordCommitConsumer {

    private static final Logger logger = LoggerFactory.getLogger(Ex2_3_RecordCommitConsumer.class);
    private static final String TOPIC_NAME = "hello-kafka";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "hello-kafka-group";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        int count = 0;

        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {

            consumer.subscribe(List.of(TOPIC_NAME));

            while (count < 10) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("=============================================");
                    logger.info("");
                    logger.info("Message received: {}", record);
                    logger.info("");
                    logger.info("=============================================");

                    // 레코드 단위로 commit 을 수행한다. 레코드마다 commit 을 수행하므로 성능이 떨어질 수 있다.
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, null));
                    consumer.commitSync(currentOffsets);
                }
                count++;
            }
        }
    }
}
