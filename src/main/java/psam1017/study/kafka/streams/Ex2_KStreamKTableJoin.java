package psam1017.study.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

@SuppressWarnings("SpellCheckingInspection")
public class Ex2_KStreamKTableJoin {

    // 소스 프로세서 - stream(), table()
    // 스트림 프로세서 - join()
    // 싱크 프로세서 - to()

    /*
    KStream : (somin, iPhone), (minsu, Galaxy)
    KTable : (minsu, Busan), (somin, Seoul)

    -> join()

    KStream : (somin, (iPhone, Seoul)), (minsu, (Galaxy, Busan))

    ---
    시나리오 : somin 의 주소가 Daegu 로 변경되면, KTable 은 마지막 레코드를 유효한 데이터로 보기 때문에 가장 최근에 바뀐 주소로 조인하여 Daegu 를 반환한다.
     */

    // 대부분의 DB 는 정적으로 저장된 데이터를 조회하는 순간에 조인하여 사용하지만
    // 카프카 스트림즈는 실시간으로 들어오는 데이터를 조인하여 스트리밍 처리할 수 있다.
    // 이를 통해 이벤트 기반 스트리밍 데이터 파이프라인을 구축할 수 있다.

    private static final String APPLICATION_ID = " kstream-ktable-join-application";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String ADDRESS_TABLE = "address";
    private static final String ORDER_STREAM = "order";
    private static final String JOIN_RESULT_STREAM = "join_result";

    @SuppressWarnings("resource")
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // order 가 KStream 이므로 order 에 값이 추가될 때마다 조인된 KTable 인 address 에서 최신 값을 가져온다.
        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> addressTable = builder.table(ADDRESS_TABLE);
        KStream<String, String> orderStream = builder.stream(ORDER_STREAM);

        // join 을 수행한다.
        orderStream.join(addressTable, (order, address) -> order + " send to " + address)
                .to(JOIN_RESULT_STREAM);

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        // join 을 수행하기 위해 파티션 개수를 동일하게 맞춰주어야 한다.
        // bin/windows/kafka-topics.bat --create --bootstrap-server localhost:9092 --partitions 3 --topic address
        // bin/windows/kafka-topics.bat --create --bootstrap-server localhost:9092 --partitions 3 --topic order
        // bin/windows/kafka-topics.bat --create --bootstrap-server localhost:9092 --partitions 3 --topic join_result

        // 마찬가지로 실습하기 위해서 3개의 terminal 에서 kafka-console-producer, kafka-console-consumer 를 실행시켜두어야 한다.
        // bin/windows/kafka-console-producer.bat --bootstrap-server localhost:9092 --topic address --property "parse.key=true" --property "key.separator=:"
        // bin/windows/kafka-console-producer.bat --bootstrap-server localhost:9092 --topic order --property "parse.key=true" --property "key.separator=:"
        // bin/windows/kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic join_result --from-beginning
    }
}
