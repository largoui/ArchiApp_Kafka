package if4030.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.io.FileInputStream;
import java.io.IOException;
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

    private static final Integer DEFAULT_TOP_NUMBER = 20;

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
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return props;
    }

    static void createWordCountCategoryStream(final StreamsBuilder builder, CountDownLatch latch) throws IOException {
        final KStream<String, String> source = builder.stream(Arrays.asList(INPUT_TOPIC, LISTEN_TOPIC));
        Map<String,Map<String, Integer>> wordCountsByCategory = new HashMap<>();

        source.foreach((key, value) -> {
            String word = value;
            String category = key;

            if (key == null && value != null) {
                String[] parts = value.split(" ");
                String command = parts[0];
                String[] args = parts.length > 1 ? Arrays.copyOfRange(parts, 1, parts.length) : null;
                switch (command) {
                    case "END":
                        handleEndCommand(args, wordCountsByCategory, latch);
                        return;
                    
                    case "DISPLAY":
                        handleDisplayCommand(args, wordCountsByCategory);
                        return;
                    
                    case "RESET":
                        handleResetCommand(args, wordCountsByCategory);
                        return;
                    
                    default:
                        return;
                }

            } else if (category != null && word != null) {
                wordCountsByCategory.putIfAbsent(category, new HashMap<>());
                Map<String, Integer> wordCounts = wordCountsByCategory.get(category);
                wordCounts.put(word, wordCounts.getOrDefault(word, 0) + 1);
            }
        });
    }

    private static void topWordsByCategory(Map<String, Map<String, Integer>> wordCountsByCategory, Integer topNumber) {
        for (Map.Entry<String, Map<String, Integer>> entry : wordCountsByCategory.entrySet()) {
            topWordsInCategory(wordCountsByCategory, entry.getKey(), topNumber);
        }
    }

    private static void topWordsInCategory(Map<String, Map<String, Integer>> wordCountsByCategory, String category, Integer topNumber) {
        if (category == null || !wordCountsByCategory.containsKey(category)) {
            return;
        }

        System.out.println("Category: " + category);
        Map<String, Integer> wordCounts = wordCountsByCategory.get(category);
        PriorityQueue<Map.Entry<String, Integer>> pq = new PriorityQueue<>(Comparator.comparingInt(Map.Entry<String, Integer>::getValue));
        for (Map.Entry<String, Integer> wordEntry : wordCounts.entrySet()) {
            pq.offer(wordEntry);
            if (pq.size() > topNumber) {
                pq.poll();
            }
        }

        while (!pq.isEmpty()) {
            Map.Entry<String, Integer> topEntry = pq.poll();
            System.out.println(topEntry.getKey() + ": " + topEntry.getValue());
        }
    }

    private static void handleEndCommand(String[] args, Map<String, Map<String, Integer>> wordCountsByCategory, CountDownLatch latch) {
        handleDisplayCommand(args, wordCountsByCategory);
        latch.countDown();
    }

    private static void handleDisplayCommand(String[] args, Map<String, Map<String, Integer>> wordCountsByCategory) {
        if (args == null) {
            topWordsByCategory(wordCountsByCategory, DEFAULT_TOP_NUMBER);
            return;
        }
        
        if (args.length == 1) {
            String arg = args[0];
            if (arg.matches("\\d+")) {
                topWordsByCategory(wordCountsByCategory, Integer.parseInt(arg));
            } else {
                topWordsInCategory(wordCountsByCategory, arg, DEFAULT_TOP_NUMBER);
            }
            return;
        }

        if (args.length == 2) {
            String arg1 = args[0];
            String arg2 = args[1];
            if (arg1.matches("\\d+")) {
                topWordsInCategory(wordCountsByCategory, arg2, Integer.parseInt(arg1));
            } else if (arg2.matches("\\d+")) {
                topWordsInCategory(wordCountsByCategory, arg1, Integer.parseInt(arg2));
            }
            return;
        }
    }

    private static void handleResetCommand(String[] args, Map<String, Map<String, Integer>> wordCountsByCategory) {
        wordCountsByCategory.clear();
    }

    public static void main(final String[] args) throws IOException {
        final Properties props = getStreamsConfig(args);

        final StreamsBuilder builder = new StreamsBuilder();
        final CountDownLatch latch = new CountDownLatch(1);
        createWordCountCategoryStream(builder, latch);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

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
        } finally {
            streams.close();
        }

        System.exit(0);
    }
}

