package de.esailors.dataheart.drillviews.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import de.esailors.dataheart.drillviews.util.CollectionUtil;

public class KafkaEventFetcher {

	private static final Logger log = LogManager.getLogger(KafkaEventFetcher.class.getName());

	private Set<String> topicsBlacklist;
	private Set<String> topicsWhitelist;

	private MessageProcessor eventProcessor;

	private Consumer<byte[], byte[]> consumer;
	private Set<Topic> topics = new HashSet<>();

	public KafkaEventFetcher() {
		initConsumer();
		initTopicList();
		initMessageProcessor();
		initShutdownHook();
	}

	public Set<Topic> fetchEvents() {
		log.info("Starting to consume events from Kafka");
		long consumeStart = System.currentTimeMillis();
		int cnt = 0;
		for (Topic topic : CollectionUtil.toSortedList(topics)) {
			cnt++;
			log.info(cnt + " / " + topics.size() + ": " + topic.toString());
			fetchEventsForTopic(topic);
		}
		long consumeEnd = System.currentTimeMillis();
		log.info("Consuming finished after " + (consumeEnd - consumeStart) + " ms");

		return topics;
	}

	private void fetchEventsForTopic(Topic topic) {

		// poll each partition individually until we have enough
		Collection<TopicPartition> topicParititions = discoverParitionsForTopic(topic);
		int consumedRecordsTotal = 0;
		for (TopicPartition topicPartition : topicParititions) {
			prepareConsumerForTopicPartition(topicPartition);
			ConsumerRecords<byte[], byte[]> consumedRecords = consumer
					.poll(Config.getInstance().KAFKA_CONSUMER_POLL_TIMEOUT);

			eventProcessor.processRecords(topic, topicPartition, consumedRecords);
			log.info(consumedRecords.count() + " / " + Config.getInstance().KAFKA_CONSUMER_MAX_POLL_RECORDS + " records consumed from " + topicPartition);
			consumedRecordsTotal += consumedRecords.count();
			if (consumedRecordsTotal >= Config.getInstance().KAFKA_CONSUMER_MAX_POLL_RECORDS) {
				log.info("Consumed enough records for " + topic.toString() + ": " + consumedRecordsTotal + " / "
						+ Config.getInstance().KAFKA_CONSUMER_MAX_POLL_RECORDS);
				break;
			}
		}
		if(consumedRecordsTotal == 0) {
			log.info("NO records consumed for " + topic.toString());
		}
	}

	private void prepareConsumerForTopicPartition(TopicPartition topicPartition) {
		if (!consumer.subscription().isEmpty()) {
			log.debug("Unsubscribing from all topic partitions");
			consumer.unsubscribe();
		}
		log.debug("Preparing for topic partitiong: " + topicPartition);
		consumer.assign(Collections.singleton(topicPartition));
		resetConsumerOffsets();
	}

	private Collection<TopicPartition> discoverParitionsForTopic(Topic topic) {
		log.debug("Preparing consumer for: " + topic);

		Collection<TopicPartition> partitions = new ArrayList<>();
		List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic.getName());
		for (PartitionInfo partitionInfo : partitionInfos) {
			TopicPartition partition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
			partitions.add(partition);
		}

		topic.setPartitionCount(partitions.size());
		log.debug("Preparation for " + topic + " done, partitions: " + partitions.size());

		return partitions;
	}

	private void resetConsumerOffsets() {
		// forcefully reset offset to 0
		consumer.assignment().forEach(topicPartition -> {
			consumer.seek(topicPartition, 0);
		});
	}

	private void initTopicList() {
		log.debug("Building topic list");

		initBlacklistTopics();
		initWhitelistTopics();

		log.debug("Listing all topics in Kafka");
		Map<String, List<PartitionInfo>> topicNames = consumer.listTopics();

		for (String topicName : topicNames.keySet()) {
			String msg = " * " + topicName + ": ";
			if (topicsWhitelist != null && (topicsWhitelist.size() > 0) && !topicsWhitelist.contains(topicName)) {
				msg += "WHITELIST IGNORED";
				log.warn(msg);
				continue;
			}
			// additionally to hardcoded list from initBlacklistTopics() we ignore all topic
			// names that start with an underscore
			if (topicName.startsWith("_")) {
				topicsBlacklist.add(topicName);
			}
			if (topicsBlacklist.contains(topicName)) {
				msg += "IGNORED";
				log.info(msg);
				continue;
			}
			topics.add(new Topic(topicName));
			msg += "ASSIGNED";
			log.debug(msg);

		}
		log.info("Processing topics " + topics.size() + " / " + topicNames.size() + " discovered");
	}

	private void initMessageProcessor() {
		this.eventProcessor = new MessageProcessor();
	}

	private void initBlacklistTopics() {
		topicsBlacklist = new HashSet<String>();
		topicsBlacklist.add("avro_schema");
		log.debug("Blacklisted topics: " + topicsBlacklist.size());
	}

	private void initWhitelistTopics() {
		// FOR DEVELOPMENT PURPOSES ONLY!
		topicsWhitelist = new HashSet<String>();
//		topicsWhitelist.add("payment_payin_processed");
//		topicsWhitelist.add("customer_restrictions_changed");
//		topicsWhitelist.add("customer_registration");
//		topicsWhitelist.add("postident_response_reporting");
//		topicsWhitelist.add("account_balance_change_events");
//		topicsWhitelist.add("alinghi_dbg_real_ticket_mapped");
		topicsWhitelist.add("alinghi_dbg_lc_ticket_ordered");

		if (topicsWhitelist.size() > 0) {
			log.warn("DEV ONLY! Whitelisted topics: " + topicsWhitelist.size());
		}
	}

	private void initConsumer() {
		consumer = new KafkaConsumer<>(initializeConsumerProperties());
	}

	private Properties initializeConsumerProperties() {
		log.info("Creating Kafka consumer for: " + Config.getInstance().KAFKA_CONSUMER_BOOTSTRAP_SERVERS);

		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.getInstance().KAFKA_CONSUMER_BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, Config.getInstance().KAFKA_CONSUMER_GROUP_ID);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Config.getInstance().KAFKA_CONSUMER_AUTO_OFFSET_RESET);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Config.getInstance().KAFKA_CONSUMER_ENABLE_AUTO_COMMIT);
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Config.getInstance().KAFKA_CONSUMER_MAX_POLL_RECORDS);
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
