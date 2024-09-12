package psam1017.study.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Ex1_SimpleProducer {

    private static final Logger logger = LoggerFactory.getLogger(Ex1_SimpleProducer.class);
    private static final String TOPIC_NAME = "hello-kafka";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {

        // 필수 옵션 지정
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Producer 객체 생성
        // KafkaProducer 는 시스템 자원을 사용하므로 사용이 끝나면 강제종료하지 말고, 안전하게 할당된 자원을 해제해야 한다.
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            // 전송할 record value
            String value = "Hello, Kafka!";

            // ProducerRecord 객체 생성
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, value);

            // send 메소드를 호출해도 즉시 전송되지는 않고 내부적으로 Accumulator 에 저장된다.
            producer.send(record);

            logger.info("=============================================");
            logger.info("");
            logger.info("Message sent: {}", record);
            logger.info("");
            logger.info("=============================================");

            // flush 메소드를 호출하면 Accumulator 에 저장된 모든 레코드를 Sender 스레드로 전송한다.
            // 이후 최종적으로 Sender 가 Broker 로 Record 를 전송한다.
            producer.flush();

            // 전송 결과를 확인하려면 kafka directory 에서 아래 명령어를 실행한다(Windows 기준).
            // bin/windows/kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic hello-producer --from-beginning
            // 실행 결과는 resources 의 이미지 참고.
        }
    }
}
