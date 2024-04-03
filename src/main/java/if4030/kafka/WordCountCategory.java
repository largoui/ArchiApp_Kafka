package if4030.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public final class WordCountCategory {

    public static final String INPUT_TOPIC = "tagged-words-stream";
    public static final String LISTEN_TOPIC = "command-topic";

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
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "streams-count-category");
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

    static void createWordCountCategoryStream(final StreamsBuilder builder) throws IOException {
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);
        Map<String,Map<String, Integer>> wordCountsByCategory = new HashMap<>();
        
        Properties props = getStreamsConfig(null);
        props.put("ConsumerConfig.GROUP_ID_CONFIG", "command-consumer");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");  
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Arrays.asList(INPUT_TOPIC, LISTEN_TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String topic = record.topic();
                if (topic.equals(INPUT_TOPIC)) {
                    source.foreach((key, value) -> {
                        String word = value;
                        String category = key;
                        wordCountsByCategory.putIfAbsent(category, new HashMap<>());
                        Map<String, Integer> wordCounts = wordCountsByCategory.get(category);
                        wordCounts.put(word, wordCounts.getOrDefault(word, 0) + 1);
                    }); 
                } else if (topic.equals(LISTEN_TOPIC)) {
                    if (record.value().equals("END")) {
                        topWordsByCategory(wordCountsByCategory);
                        kafkaConsumer.close();
                        return;
                    }
                }
            }
        }
    }

    private static void topWordsByCategory(Map<String, Map<String, Integer>> wordCountsByCategory) {
        for (Map.Entry<String, Map<String, Integer>> entry : wordCountsByCategory.entrySet()) {
            System.out.println("Category: " + entry.getKey());
            Map<String, Integer> wordCounts = entry.getValue();
            PriorityQueue<Map.Entry<String, Integer>> pq = new PriorityQueue<>(
                    Comparator.comparingInt(Map.Entry::getValue));
            for (Map.Entry<String, Integer> wordEntry : wordCounts.entrySet()) {
                pq.offer(wordEntry);
                if (pq.size() > 20) {
                    pq.poll();
                }
            }
            
            while (!pq.isEmpty()) {
                Map.Entry<String, Integer> topEntry = pq.poll();
                System.out.println(topEntry.getKey() + ": " + topEntry.getValue());
            }
        }
    }

    public static void main(final String[] args) throws IOException {
        final Properties props = getStreamsConfig(args);

        final StreamsBuilder builder = new StreamsBuilder();
        createWordCountCategoryStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-count-category-shutdown-hook") {
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

