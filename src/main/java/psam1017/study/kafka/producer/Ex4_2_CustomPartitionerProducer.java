package psam1017.study.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Ex4_2_CustomPartitionerProducer {

    private static final String TOPIC_NAME = "hello-kafka";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 대부분 기본 Partitioner 를 사용하면 되지만, 특정한 Partitioner 를 사용하고 싶다면 직접 구현해야 한다.
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, Ex4_1_CustomPartitioner.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            // partition 번호를 지정하여 전송할 수 있다.
            int partition = 0;
            String key = "name";
            String value = "psam1017";

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, partition, key, value);
            producer.send(record);
            producer.flush();

            // 전송 결과를 확인하려면 kafka directory 에서 아래 명령어를 실행한다(Windows 기준).
            // bin/windows/kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic hello-producer --from-beginning --property print.key=true --property key.separator=:
            // 실행 결과는 resources 의 이미지 참고.
        }
    }
}
