package de.esailors.dataheart.drillviews.kafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;

public class KafkaConsumerFactory {

	private static final Logger log = LogManager.getLogger(KafkaConsumerFactory.class.getName());
	
	private static KafkaConsumerFactory instance;
	
	public static KafkaConsumerFactory getInstance() {
		if(instance == null) {
			instance = new KafkaConsumerFactory();
		}
		return instance;
	}
	
	private Properties consumerProperties;
	
	private KafkaConsumerFactory() {
		initializeConsumerProperties();
	}
	
	public KafkaConsumer<byte[], byte[]> createConsumer() {
		return new KafkaConsumer<>(consumerProperties);
	}
	
	private void initializeConsumerProperties() {
		log.info("Creating Kafka consumer for: " + Config.getInstance().KAFKA_CONSUMER_BOOTSTRAP_SERVERS);

		consumerProperties = new Properties();
		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.getInstance().KAFKA_CONSUMER_BOOTSTRAP_SERVERS);
		consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, Config.getInstance().KAFKA_CONSUMER_GROUP_ID);
		consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Config.getInstance().KAFKA_CONSUMER_AUTO_OFFSET_RESET);
		consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Config.getInstance().KAFKA_CONSUMER_ENABLE_AUTO_COMMIT);
		consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Config.getInstance().KAFKA_CONSUMER_MAX_POLL_RECORDS);
	}
	
}
