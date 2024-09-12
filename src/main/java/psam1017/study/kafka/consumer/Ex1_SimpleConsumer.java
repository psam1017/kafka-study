package psam1017.study.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Ex1_SimpleConsumer {

    private static final Logger logger = LoggerFactory.getLogger(Ex1_SimpleConsumer.class);
    private static final String TOPIC_NAME = "hello-kafka";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "hello-kafka-group";

    public static void main(String[] args) {

        // 필수 옵션 지정
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 예제임을 감안하여 10초만 유지하고 종료한다. 실제 환경에서는 끊임 없이 서버를 유지하면 된다.
        int count = 0;

        // Consumer 객체 생성
        // KafkaConsumer 는 시스템 자원을 사용하므로 사용이 끝나면 강제종료하지 말고, 안전하게 할당된 자원을 해제해야 한다.
        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {

            // 구독할 topic 지정
            consumer.subscribe(List.of(TOPIC_NAME));

            while (count < 10) {
                // poll 메소드를 호출하면 Consumer 가 Broker 로부터 레코드를 가져온다.
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

            // 실행 이후에도 동작 여부를 확인하려면 kafka directory 에서 아래 명령어를 실행한다(Windows 기준).
            // bin/windows/kafka-console-producer.bat --bootstrap-server localhost:9092 --topic hello-kafka
            // >Hello, Kafka!
            // >...

            // 실행 결과는 resources 의 이미지 참고.
        }
    }
}
