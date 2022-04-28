package com.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.Properties;

public class wordCount {

    public static void main(String[] args) {

        String applicationId = "kafka-stream-words";
        String bootStrapServers = "localhost:9092";
        String inputTopic = "word-count-input";
        String outputTopic = "word-count-output";

        // properties file for config of kafkaStreams
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        // build the topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // 1. stream from the kafka topic
        KStream<String, String> wordCountInput = streamsBuilder.stream(inputTopic);

        KTable<String, Long> wordCounts = wordCountInput
        // 2. MapValues to lowercase
                .mapValues(value -> value.toLowerCase())
        // 3. FlatMapValues split by space
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
        // 4. SelectKey to apply a key
                .selectKey((key, value) -> value)
        // 5. GroupByKey aggregation by key
                .groupByKey()
        // 6. Count the occurrences in each group
                .count(Named.as("Counts"));

        // 7. send the result TO output topic
        wordCounts.toStream().to(outputTopic);

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), config);
        kafkaStreams.start();

        // printed the topology
        System.out.println(kafkaStreams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
