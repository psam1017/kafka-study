package psam1017.study.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

@SuppressWarnings({"SpellCheckingInspection", "resource"})
public class Ex6_2_Processor {

    /*
    스트림즈DSL 은 데이터 처리, 분기, 조인을 위한 다양한 메서드들을 제공하지만 추가적인 상세 로직 구현이 필요하다면 프로세서 API 를 활용할 수 있다.
    즉, 프로세서 API 가 좀 더 추상화된 레벨의 객체이다.
    프로세서 API 에는 KStream, KTable, GlobalKTable 개념이 없다는 점을 유의해야 한다. 단, 스트림즈DSL 과 프로세서 API 를 함께 구현하여 활용할 수는 있다.
    프로세서 API 를 구현하기 위해서는 Processor 또는 Transformer 인터페이스를 구현해야 한다.
    Processor 는 일정 로직이 이루어진 뒤 다음 프로세서로 데이터가 넘어가지 않을 때 사용한다.
    Transformer 는 일정 로직이 이루어진 뒤 다음 프로세서로 데이터를 넘길 때 사용한다.
     */

    private static final String APPLICATION_NAME = "processor-application";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String STREAM_LOG = "stream_log";
    private static final String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = new Topology();
        topology.addSource("Source", STREAM_LOG)
                .addProcessor("Process", Ex6_1_FilterProcessor::new, "Source") // deprecated
                .addSink("Sink", STREAM_LOG_FILTER, "Process");

        KafkaStreams streaming = new KafkaStreams(topology, props);
        streaming.start();
    }
}
