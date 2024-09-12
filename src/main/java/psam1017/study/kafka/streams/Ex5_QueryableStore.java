package psam1017.study.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

@SuppressWarnings("resource")
public class Ex5_QueryableStore {

    /*
    카프카 스트림즈에서 KTable 은 카프카 토픽의 데이터를 로컬의 rocksDB 에 Materialized View 로 만들어 두고 사용하기 때문에 레코드의 메시지 키, 값을 기반으로 KeyValueStore 로 사용할 수 있다.
    특정 토픽을 KTable 로 사용하고 ReadOnlyKeyValueStore 로 뷰를 가져오면 메시지 키를 기반으로 토픽 데이터를 조회할 수 있게 된다.
    카프카를 사용하여 로컬 캐시를 구현한 것과 유사하다.
     */

    private static final Logger logger = LoggerFactory.getLogger(Ex5_QueryableStore.class);
    private static final String APPLICATION_ID = " queryable-store-application";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String MY_STREAM = "my_stream";
    private static boolean initialize = false;
    private static ReadOnlyKeyValueStore<String, String> keyValueStore;

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> addressTable = builder.table(MY_STREAM, Materialized.as(MY_STREAM));
        KafkaStreams streams;
        streams = new KafkaStreams(builder.build(), props);
        streams.start();

        TimerTask task = new TimerTask() {
            public void run() {
                if (!initialize) {
                    keyValueStore = streams.store(StoreQueryParameters.fromNameAndType(MY_STREAM,
                            QueryableStoreTypes.keyValueStore()));
                    initialize = true;
                }
                printKeyValueStoreData();
            }
        };
        Timer timer = new Timer("Timer");
        long delay = 10000L;
        long interval = 1000L;
        timer.schedule(task, delay, interval);
    }

    static void printKeyValueStoreData() {
        logger.info("========================");
        KeyValueIterator<String, String> address = keyValueStore.all();
        address.forEachRemaining(keyValue -> logger.info(keyValue.toString()));
    }
}
