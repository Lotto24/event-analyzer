package de.esailors.dataheart.drillviews.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.Topic;

public class KafkaEventFetcher {

	private static final Logger log = LogManager.getLogger(KafkaEventFetcher.class.getName());

	private static Collection<String> topicsBlacklist;

	private Config config;
	private MessageProcessor eventProcessor;

	private Consumer<byte[], byte[]> consumer;
	private Set<Topic> topics = new HashSet<>();
	
	public KafkaEventFetcher(Config config) {
		this.config = config;

		initConsumer();
		initTopicList();
		initMessageProcessor();
		initShutdownHook();
	}
	
	public Set<Topic> fetchEvents() {
		log.info("Consuming events from Kafka");
		long consumeStart = System.currentTimeMillis();
		for (Topic topic : topics) {
			fetchEventsForTopic(topic);
		}
		long consumeEnd = System.currentTimeMillis();
		log.info("Consuming finished after " + (consumeEnd - consumeStart) + " ms");
		
		return topics;
	}
	
	private void fetchEventsForTopic(Topic topic) {
		prepareConsumerFor(topic);
		ConsumerRecords<byte[], byte[]> consumedRecords = consumer.poll(config.KAFKA_CONSUMER_POLL_TIMEOUT);
		eventProcessor.processRecords(topic, consumedRecords);
	}
	

	private void prepareConsumerFor(Topic topic) {
		log.debug("Preparing for: " + topic);
		// reset first
		if (!consumer.subscription().isEmpty()) {
			log.debug("Unsubscribing from all topics");
			consumer.unsubscribe();
		}

		Collection<TopicPartition> partitions = new ArrayList<>();
		List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic.getTopicName());
		for (PartitionInfo partitionInfo : partitionInfos) {
			TopicPartition partition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
			partitions.add(partition);
		}

		consumer.assign(partitions);

		// forcefully reset offset to 0
		consumer.assignment().forEach(topicPartition -> {
			consumer.seek(topicPartition, 0);
		});

		topic.setPartitionCount(partitions.size());
		log.debug("Preparation for " + topic + " done, partitions: " + partitions.size());

	}

	private void initTopicList() {
		log.debug("Building topic list");

		initBlacklistTopics();

		Map<String, List<PartitionInfo>> topicNames = consumer.listTopics();

		for (String topicName : topicNames.keySet()) {
			String msg = " * " + topicName + ": ";
			if (topicsBlacklist.contains(topicName)) {
				msg += "IGNORED";
				log.debug(msg);
				continue;
			}
			topics.add(new Topic(topicName));
			msg += "ASSIGNED";
			log.debug(msg);

		}
		log.info("Topics to process: " + topicNames.size());
	}
	
	private void initMessageProcessor() {
		this.eventProcessor = new MessageProcessor(config);
	}

	private void initBlacklistTopics() {
		topicsBlacklist = new HashSet<String>();
		topicsBlacklist.add("__consumer_offsets");
		topicsBlacklist.add("avro_schema");
		topicsBlacklist.add("test_events");
		topicsBlacklist.add("test");
		log.debug("Blacklisted topics: " + topicsBlacklist.size());
	}

	private void initConsumer() {

		log.info("Creating Kafka consumer for: " + config.KAFKA_CONSUMER_BOOTSTRAP_SERVERS);
		consumer = new KafkaConsumer<>(initializeConsumerProperties());
	}

	private Properties initializeConsumerProperties() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.KAFKA_CONSUMER_BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, config.KAFKA_CONSUMER_GROUP_ID);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.KAFKA_CONSUMER_AUTO_OFFSET_RESET);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.KAFKA_CONSUMER_ENABLE_AUTO_COMMIT);
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.KAFKA_CONSUMER_MAX_POLL_RECORDS);
		return props;
	}

	public void close() {
		log.debug("Closing connection to Kafka");
		if (consumer == null) {
			log.warn("Tried to close uninitialized kafka connection");
			return;
		}
		consumer.close();
	}

	private void initShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				log.debug("Shutdown Hook triggered");
				close();
			}
		});
	}

}
