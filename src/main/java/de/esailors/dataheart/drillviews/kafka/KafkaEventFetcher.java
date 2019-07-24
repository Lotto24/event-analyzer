package de.esailors.dataheart.drillviews.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.Topic;

public class KafkaEventFetcher extends Thread {

	private static final Logger log = LogManager.getLogger(KafkaEventFetcher.class.getName());
	
	private Consumer<byte[], byte[]> consumer;
	
	public KafkaEventFetcher(Runnable r, String name) {
		super(r, name);
		this.consumer = KafkaConsumerFactory.getInstance().createConsumer();
		
		log.info("Created fetcher thread for " + r);
	}
	
	public void fetchEventsForTopic(Topic topic) {

		Collection<TopicPartition> topicParititions = discoverParitionsForTopic(topic);
		int consumedRecordsTotal = 0;

		if (Config.getInstance().KAFKA_POLL_PARTITIONS_INDIVIDUALLY) {
			// poll each partition individually until we have enough
			for (TopicPartition topicPartition : topicParititions) {
				prepareConsumerForTopicPartition(topicPartition, consumer);
				ConsumerRecords<byte[], byte[]> consumedRecords = consumer
						.poll(Config.getInstance().KAFKA_CONSUMER_POLL_TIMEOUT);

				MessageProcessor.getInstance().processRecords(topic, consumedRecords);
				log.info(consumedRecords.count() + " / " + Config.getInstance().KAFKA_CONSUMER_MAX_POLL_RECORDS
						+ " records consumed from " + topicPartition);
				consumedRecordsTotal += consumedRecords.count();
				if (consumedRecordsTotal >= Config.getInstance().KAFKA_CONSUMER_MAX_POLL_RECORDS) {
					log.info("Consumed enough records for " + topic.toString() + ": " + consumedRecordsTotal + " / "
							+ Config.getInstance().KAFKA_CONSUMER_MAX_POLL_RECORDS);
					break;
				}
			}
		} else {
			prepareConsumerForTopicPartitions(topicParititions, consumer);
			ConsumerRecords<byte[], byte[]> consumedRecords = consumer
					.poll(Config.getInstance().KAFKA_CONSUMER_POLL_TIMEOUT);

			MessageProcessor.getInstance().processRecords(topic, consumedRecords);
			consumedRecordsTotal += consumedRecords.count();
			log.info(consumedRecords.count() + " / " + Config.getInstance().KAFKA_CONSUMER_MAX_POLL_RECORDS
					+ " records consumed");
		}
		if (consumedRecordsTotal == 0) {
			log.info("NO records consumed for " + topic.toString());
		}
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

	private void prepareConsumerForTopicPartition(TopicPartition topicPartition, Consumer<byte[], byte[]> consumer) {
		log.debug("Preparing for topic partitiong: " + topicPartition);
		prepareConsumerForTopicPartitions(Collections.singleton(topicPartition), consumer);
	}
	
	private void prepareConsumerForTopicPartitions(Collection<TopicPartition> topicPartitions, Consumer<byte[], byte[]> consumer) {
		if (!consumer.subscription().isEmpty()) {
			log.debug("Unsubscribing from all topic partitions");
			consumer.unsubscribe();
		}
		consumer.assign(topicPartitions);
		resetConsumerOffsets();
	}
	
	private void resetConsumerOffsets() {
		// forcefully reset offset to 0
		consumer.assignment().forEach(topicPartition -> {
			consumer.seek(topicPartition, 0);
		});
	}

	public void close() {
		log.debug("Closing connection to Kafka from fetcher");
		if (consumer == null) {
			log.warn("Tried to close uninitialized kafka connection");
			return;
		}
		consumer.close();
	}
}
