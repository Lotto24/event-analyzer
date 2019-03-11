package de.esailors.dataheart.drillviews.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.Event;
import de.esailors.dataheart.drillviews.data.Topic;

public class MessageProcessor {

	private static final Logger log = LogManager.getLogger(MessageProcessor.class.getName());

	private Config config;
	private EventFactory eventFactory;

	public MessageProcessor(Config config) {
		this.config = config;
		this.eventFactory = new EventFactory(config);
	}

	public void processRecords(Topic topic, ConsumerRecords<byte[], byte[]> consumedRecords) {
		if (consumedRecords.count() == 0) {
			log.warn("Did not receive any event for " + topic);
		} else {
			log.info("Received " + consumedRecords.count() + " messages for: " + topic);
		}
		for (ConsumerRecord<byte[], byte[]> record : consumedRecords) {
			Event event = eventFactory.buildEvent(topic, record);
			topic.addEvent(event);
		}
	}

}
