package psam1017.study.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Ex2_1_AutoCommitConsumer {

    private static final Logger logger = LoggerFactory.getLogger(Ex2_1_AutoCommitConsumer.class);
    private static final String TOPIC_NAME = "hello-kafka";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "hello-kafka-group";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 오프셋과 관련된 설정
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); // default
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000); // default

        int count = 0;

        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {

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

                // poll 메소드를 호출한 이후에 commitSync 메소드를 호출하면 현재 poll 메소드에서 가져온 레코드의 오프셋을 직접 커밋한다.
                consumer.commitSync();
                count++;
            }
        }
    }
}
