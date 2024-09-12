package psam1017.study.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class Ex3_RebalanceListenerConsumer {

    private static final Logger logger = LoggerFactory.getLogger(Ex3_RebalanceListenerConsumer.class);
    private static final String TOPIC_NAME = "hello-kafka";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "hello-kafka-group";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        int count = 0;

        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {

            // 리밸런스 발생을 감지하는 ConsumerRebalanceListener 인터페이스를 구현할 수 있다.
            consumer.subscribe(List.of(TOPIC_NAME), new ConsumerRebalanceListener() {

                // 리밸런스가 끝난 이후 파티션 할당이 완료되면 호출된다.
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    logger.warn("Partitions are assigned : " + partitions.toString());
                }

                // 리밸런스가 시작되기 전에 호출된다.
                // 리밸런스 시작 직전에 마지막으로 처리한 레코드를 안전하게 수동 커밋하는 식으로 활용할 수 있다.
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    logger.warn("Partitions are revoked : " + partitions.toString());
                    consumer.commitSync();
                }
            });

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
        }

        // 한 번에 2개의 main 메소드를 실행하면 리밸런스가 발생하는 것을 테스트해볼 수 있다.
    }
}
