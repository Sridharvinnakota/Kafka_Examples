package kafka;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import org.apache.kafka.common.serialization.StringDeserializer;


public class Consumer  {
   
    public static void main(String[] args) {
       KafkaConsumer<Integer, String> consumer = null;
    	 String topic = "test";
    	  Properties props = new Properties();
          props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
          props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
          props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
          props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
          props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
          props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
          props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
          consumer = new KafkaConsumer(props);
          consumer.subscribe(Collections.singletonList(topic));
          while(true)
          {
          ConsumerRecords<Integer, String> records = consumer.poll(1000);
          for (ConsumerRecord<Integer, String> record : records) {
              System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
          }
          }
        
	}
}
