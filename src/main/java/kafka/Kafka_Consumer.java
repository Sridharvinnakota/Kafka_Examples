package kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Kafka_Consumer {

	public static void main(String[] args) {
Properties properties = new Properties();
		
		// kafka bootstrap server
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");		
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());		
		properties.setProperty("value.deserializer", StringDeserializer.class.getName());
		
		properties.setProperty("group.id", "test");
		properties.setProperty("enable.auto.commit", "true");
		properties.setProperty("auto.commit.interval.ms", "1000");
		properties.setProperty("auto.offset.reset", "earliest");
		
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
		kafkaConsumer.subscribe(Arrays.asList("test"));
		
		while(true)
		{
			ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
			for(ConsumerRecord<String, String> consumerRecord : consumerRecords )
			{
				consumerRecord.value();
				consumerRecord.offset();
				consumerRecord.key();
				consumerRecord.partition();
				consumerRecord.topic();
				consumerRecord.timestamp();
				
				System.out.println("Partition "+consumerRecord.partition() +
									", Offset "+consumerRecord.offset()+
									", Key "+consumerRecord.key() +
									", Value"+consumerRecord.value());
			}
			
		}
	}
}