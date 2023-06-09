package de.esailors.dataheart.drillviews.kafka;

import java.io.IOException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.data.Event;
import de.esailors.dataheart.drillviews.data.Topic;
import de.esailors.dataheart.drillviews.exception.UnknownSchemaException;

public class MessageProcessor {

	private static final Logger log = LogManager.getLogger(MessageProcessor.class.getName());

	private static MessageProcessor instance;
	
	public static MessageProcessor getInstance() {
		if(instance == null) {
			instance = new MessageProcessor();
		}
		return instance;
	}
	
	private EventFactory eventFactory;

	private MessageProcessor() {
		this.eventFactory = new EventFactory();
	}

	public void processRecords(Topic topic, ConsumerRecords<byte[], byte[]> consumedRecords) {
		if (consumedRecords.count() == 0) {
			return;
		}
		for (ConsumerRecord<byte[], byte[]> record : consumedRecords) {
			if(record.value() == null) {
				log.warn("Reveived record with value: null");
				topic.addBrokenMessage(null);
				continue;
			}
			try {
				Event event = eventFactory.buildEvent(topic, record);
				topic.addEvent(event);
			} catch (IOException | UnknownSchemaException e) {
				log.warn("Unable to process Kafka message to Event", e);
				topic.addBrokenMessage(record.value());
			}
		}
	}

}
