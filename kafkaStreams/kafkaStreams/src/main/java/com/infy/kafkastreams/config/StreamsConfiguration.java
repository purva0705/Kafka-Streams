package com.infy.kafkastreams.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
 
import java.util.Arrays;
import java.util.Properties;
 
public class StreamsConfiguration {
 
    public static void main(final String[] args) throws Exception {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "StreamsConfiguration");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
 
      //building kafka streams 
        StreamsBuilder builder = new StreamsBuilder(); 
        
        
        KStream<String, String> topic1data = builder.stream("topic1");
        KStream<String, String> topic2data = builder.stream("topic2");
        
        
        KTable<String, Long> wordCountsTopic1 = topic1data
            .flatMapValues(data -> Arrays.asList(data.toLowerCase().split(" ")))
            .groupBy((key, word) -> word)
            .count();
        
        
        KTable<String, Long> wordCountsTopic2 = topic2data
                .flatMapValues(data -> Arrays.asList(data.toLowerCase().split(" ")))
                .groupBy((key, word) -> word)
                .count();
         
        
		/*
		 * KTable<String, Tuple> aggregatedStream = groupedStream.aggregate( () -> new
		 * Tuple (0,0), // initializer (aggKey, newValue, aggValue) -> new Tuple
		 * (aggValue.occ + 1, aggValue.sum + Integer.parseInt(newValue))
		 * ,Materialized.with(keySerde, tupleSerde));
		 */
        
        
        wordCountsTopic1.toStream().to("topic3", Produced.with(Serdes.String(), Serdes.Long()));
        
        
        
       
        
        
        //starting kafka stream
        KafkaStreams streams = new KafkaStreams(builder.build(), config); 
        streams.start();
    }
 
}