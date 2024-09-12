package psam1017.study.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class Ex1_FilteringStreams {

    // stream_log 토픽에서 읽기
    // 소스 프로세서 - stream()
    // 스트림 프로세서 - filter()
    // 싱크 프로세서 - to()
    // stream_log_filter 토픽에 쓰기

    private static final String APPLICATION_ID = " streams-filter-application";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String STREAM_LOG = "stream_log";
    private static final String STREAM_LOG_FILTER = "stream_log_filter";

    @SuppressWarnings("resource")
    public static void main(String[] args) {

        // 옵션 설정
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 스트림 빌더 생성 및 로직 구현
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(STREAM_LOG);
        KStream<String, String> filteredStream = stream.filter((key, value) -> value.length() > 5);
        filteredStream.to(STREAM_LOG_FILTER);

        // stream.filter((key, value) -> value.length() > 5).to(STREAM_LOG_FILTER);

        // 카프카 스트림 실행
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        /*
        test 하기 위해서는
        1. streams.close() 하지 않고 계속 실행시켜두어야 한다.
        2. kafka-console-producer 로 stream_log 토픽을 띄우고 있어야 한다.
        3. kafka-console-consumer 로 stream_log_filter 토픽을 띄우고 있어야 한다.
        4. 그 상태에서 stream_log 토픽에 메시지를 보내면, stream_log_filter 토픽에 필터링된 메시지가 나타난다.
        */
    }
}
