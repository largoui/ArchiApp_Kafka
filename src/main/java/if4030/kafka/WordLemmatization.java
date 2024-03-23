package if4030.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public final class WordLemmatization {

    public static final String INPUT_TOPIC = "words-stream";
    public static final String OUTPUT_TOPIC = "tagged-words-stream";

    static Properties getStreamsConfig(final String[] args) throws IOException {
        final Properties props = new Properties();
        if (args != null && args.length > 0) {
            try (final FileInputStream fis = new FileInputStream(args[0])) {
                props.load(fis);
            }
            if (args.length > 1) {
                System.out.println("Warning: Some command line arguments were ignored. This demo only accepts an optional configuration file.");
            }
        }
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "streams-lemmatization");
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.putIfAbsent(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    static void createLineFilterStream(final StreamsBuilder builder) throws IOException {
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);
        Map<String,List<String>> lexique = Lexique();

        final KStream<String, String> lemmeStream = source
        .map((key, value) -> {
            String word;
            String category;
            if (lexique.containsKey(value)) {
                word = lexique.get(value).get(0);
                category = lexique.get(value).get(1);
            } else {
                word = value;
                category = null;
            }
            System.out.println(category +  " " + word);
            return new KeyValue<String,String>(category, word);
        });
        lemmeStream.peek(( key, word ) -> System.out.println( "key: " + key + " - word: " + word ));
        lemmeStream.to(OUTPUT_TOPIC,Produced.with(Serdes.String(), Serdes.String()));
    }

    static Map<String,List<String>> Lexique() throws FileNotFoundException {
        Map<String,List<String>> lexique = new HashMap<>();
        String fileName = "Lexique.csv";
        try (BufferedReader reader = new BufferedReader( new FileReader( fileName ))) {
            String line;
            line = reader.readLine();
            while(line != null){
                String[] wordDesc = line.split(";");
                if (wordDesc.length>2){
                    lexique.put(wordDesc[0],Arrays.asList(wordDesc[1],wordDesc[2]));
                }
                line = reader.readLine();
            } 
        } catch (FileNotFoundException e) {
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lexique;
    }

    public static void main(final String[] args) throws IOException {
        final Properties props = getStreamsConfig(args);

        final StreamsBuilder builder = new StreamsBuilder();
        createLineFilterStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-lemmatization-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}

