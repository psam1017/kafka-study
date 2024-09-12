package psam1017.study.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class Ex4_WindowProcessing {

    private static final Logger logger = LoggerFactory.getLogger(Ex4_WindowProcessing.class);
    private static final String APPLICATION_ID = " window-processing-application";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String MY_STREAM = "my_stream";

    @SuppressWarnings("resource")
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 5초 간격으로 카운트를 연산할 수 있다.
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(MY_STREAM);
        KTable<Windowed<String>, Long> countTable = stream.groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
                .count();

        countTable.toStream()
                .foreach((key, value) -> logger.info("{} is [{} ~ {}] count : {}", key.key(), key.window().startTime(), key.window().endTime(), value));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        // 주의사항
        // 커밋을 수행할 때 윈도우 사이즈가 종료되지 않아도 중간 정산 데이터를 출력한다. -> 윈도우 사이즈 종료시점에도 출력, 커밋될 때도 출력(중복 출력)
        // 커밋 시점마다 윈도우 연산의 데이터를 출력하기 때문에 동일 윈도우 사이즈의 데이터가 2개 이상 출력될 수 있다.
        // 최종적으로 윈도우에 맞는 데이터를 출력하고 싶다면 Windowed 를 기준으로 동일 윈도우 시간 데이터는 겹쳐쓰기(upsert) 방식으로 처리하는 것이 좋다.
    }
}
