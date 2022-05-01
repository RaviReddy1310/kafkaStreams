package com.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class favoriteColor {

    public static void main(String[] args) {

        String applicationId = "kafka-streams-color";
        String bootStrapServer = "localhost:9092";
        String inputTopic = "favorite-color-input";
        String intermediaryTopic = "favorite-color-intermediary";
        String outputTopic = "favorite-color-output";

        Set<String> colorSet = new HashSet<>(Arrays.asList("red", "green", "blue"));

        // config for the streams
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // get streams from inputTopic
        KStream<String, String> inputStreams = streamsBuilder.stream(inputTopic);

        // changing the stream
        KStream<String, String> inputToIntermediaryTopic = inputStreams
                .filter((key, value) -> value.contains(","))
            // changing key and value ex: <null, "ravi,red"> becomes <"ravi", "red">
                .map((key, value) -> {
                    List<String> temp = Arrays.asList(value.split(","));
                    return KeyValue.pair(temp.get(0), temp.get(1)); })
            // filtering only rbg color values
                .filter((key, value) -> colorSet.contains(value));

        // sending to an intermediaryTopic to get KTable
        inputToIntermediaryTopic.to(intermediaryTopic);

        // get table from intermediaryTopic
        KTable<String, String> inputFromIntermediaryTopic = streamsBuilder.table(intermediaryTopic);

        KTable<String , Long> favoriteColorCount = inputFromIntermediaryTopic
            // grouping by value i.e., color and getting count
                .groupBy((key, value) -> KeyValue.pair(value, value))
                .count();

        // streaming the count to outputTopic
        favoriteColorCount.toStream().to(outputTopic);

        // creating the kafkaStream application
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), config);

        // starting the kafkaStream application
        kafkaStreams.start();

        // stdout the details of kafkaStream application
        System.out.println(kafkaStreams.toString());

        // shutdown hook to close the kafkaStream application
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}
