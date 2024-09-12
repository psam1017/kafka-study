package psam1017.study.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

public class Ex5_AsyncCallbackProducer {

    private static final Logger logger = LoggerFactory.getLogger(Ex5_AsyncCallbackProducer.class);
    private static final String TOPIC_NAME = "hello-kafka";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "1");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            // 레코드를 전송한 이후 Callback 을 통해 동기로 전송 결과를 확인할 수 있다.
            // 실행 결과는 resources 의 이미지 참고.
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "name", "psam1017");
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("metadata = {}", metadata.toString());
                } else {
                    logger.error("exception = {}", exception.getMessage());
                }
            });
            producer.flush();

            // acks 기본값은 1 이다.
            // 이 경우 리더 파티션에 전송한 결과가 성공하면 전송 성공으로 간주한다.

            // acks 를 0 으로 설정하면 전송 성공 여부를 확인할 수 없다.
            // 따라서 metadata 에서 offset 이 -1 로 반환되는데, 이는 응답 결과를 받지 않으므로 offset 을 알 수 없기 때문이다.
            // 실행 결과는 resources 의 이미지 참고.
        }
    }
}
