package JsonToAvroTransformations;
import ioc.london.olympians;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;


import java.io.*;
import java.util.Properties;
import java.util.logging.Logger;

public class LondonOlympians {
    private static Logger logger;

    public static void main(String[] args) throws FileNotFoundException {
        logger = Logger.getLogger(LondonOlympians.class.getName());

        final Properties kafkaConfig = buildStreamConfiguration();
        final Properties topicConfig = buildTopicConfiguration();
        if (kafkaConfig == null || topicConfig == null)
            return;

        final String jsonTopic = topicConfig.getProperty("json-topic");
        final String avroTopic = topicConfig.getProperty("avro-topic");

        final ObjectMapper objectMapper = new ObjectMapper();

        StreamsBuilder builder = new StreamsBuilder();
        //Read from the Source Topic

        final KStream<String, String> jsonToAvroStream = builder.stream(
                jsonTopic,
                Consumed.with(Serdes.String(),
                        Serdes.String()));

        jsonToAvroStream.mapValues(v -> {
            olympians athletes = null;

            try {
                final JsonNode jsonNode = objectMapper.readTree(v);

                athletes = new olympians(
                        jsonNode.get("first_name").asText(),
                        jsonNode.get("last_name").asText(),
                        jsonNode.get("sport").asText(),
                        jsonNode.get("nation").asText());
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            return athletes;
        }).filter((k,v) -> v != null).to(avroTopic);

        final KafkaStreams streams = new KafkaStreams(builder.build(), kafkaConfig);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run () {
                streams.close();
            }
        }));
    }

    private static Properties buildStreamConfiguration() throws FileNotFoundException {
        logger.info("loading Kafka streams configs. Oh man KSQL sounds so good right now!");
        File configFile = new File("./src/main/resources/streams-config.properties");

        InputStream inputStream = new FileInputStream(configFile);

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "3");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000");

        try {
            streamsConfiguration.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            logger.severe("told you KSQL is the way t go!");
            return null;
        }

        logger.info("Well, I guess Kafka Streams is great! Loading up configs :)");
        return streamsConfiguration;
    }

    private static Properties buildTopicConfiguration() throws FileNotFoundException {
        logger.info ("Loading up the topic configs!");
        File configFile = new File("./src/main/resources/topics.properties");
        InputStream inputStream = new FileInputStream(configFile);
        Properties topicConfiguration = new Properties();

        try {
            topicConfiguration.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            logger.severe("I love KSQL!");
            return null;
        }
        logger.info("topic configuration loaded...");
        return topicConfiguration;

    }


}
