package psam1017.study.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

@SuppressWarnings("SpellCheckingInspection")
public class Ex3_KStreamGlobalKTableJoin {

    // 소스 프로세서 - stream(), globalTable()
    // 스트림 프로세서 - join()
    // 싱크 프로세서 - to()

    private static final String APPLICATION_ID = " kstream-globalktable-join-application";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String ADDRESS_TABLE = "address_global";
    private static final String ORDER_STREAM = "order";
    private static final String JOIN_RESULT_STREAM = "join_result";

    @SuppressWarnings("resource")
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> orderStream = builder.stream(ORDER_STREAM);
        GlobalKTable<String, String> addressGlobalTable = builder.globalTable(ADDRESS_TABLE);

        // join 을 수행한다.
        orderStream.join(
                        addressGlobalTable, // 조인 대상
                        (orderKey, orderValue) -> orderKey, // 조인 키
                        (order, address) -> order + " send to " + address // 조인 결과
                )
                .to(JOIN_RESULT_STREAM);

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        // 실습하기 위해 파티션 개수를 일부러 다르게 맞춰주어야 한다.
        // bin/windows/kafka-topics.bat --create --bootstrap-server localhost:9092 --partitions 2 --topic address_global
        // bin/windows/kafka-topics.bat --create --bootstrap-server localhost:9092 --partitions 3 --topic order
        // bin/windows/kafka-topics.bat --create --bootstrap-server localhost:9092 --partitions 3 --topic join_result

        // 마찬가지로 실습하기 위해서 3개의 terminal 에서 kafka-console-producer, kafka-console-consumer 를 실행시켜두어야 한다.
        // bin/windows/kafka-console-producer.bat --bootstrap-server localhost:9092 --topic address_global --property "parse.key=true" --property "key.separator=:"
        // bin/windows/kafka-console-producer.bat --bootstrap-server localhost:9092 --topic order --property "parse.key=true" --property "key.separator=:"
        // bin/windows/kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic join_result --from-beginning
    }
}
