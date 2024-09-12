package psam1017.study.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Ex2_KeyValueProducer {

    private static final String TOPIC_NAME = "hello-kafka";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            // key 와 value 를 지정하여 전송할 수 있다.
            ProducerRecord<String, String> record1 = new ProducerRecord<>(TOPIC_NAME, "name", "psam1017");
            producer.send(record1);
            ProducerRecord<String, String> record2 = new ProducerRecord<>(TOPIC_NAME, "job", "developer");
            producer.send(record2);

            producer.flush();

            // 전송 결과를 확인하려면 kafka directory 에서 아래 명령어를 실행한다(Windows 기준).
            // bin/windows/kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic hello-producer --from-beginning --property print.key=true --property key.separator=:
            // 실행 결과는 resources 의 이미지 참고.
        }
    }
}
