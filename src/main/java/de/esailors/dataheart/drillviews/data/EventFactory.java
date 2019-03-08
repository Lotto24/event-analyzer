package de.esailors.dataheart.drillviews.data;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonNode;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.kafka.MessageParser;

public class EventFactory {
	
	private static final Logger log = LogManager.getLogger(EventFactory.class.getName());

	private Config config;
	private MessageParser messageParser;
	
	public EventFactory(Config config) {
		this.config = config;

		initMessageParser();
	}

	private void initMessageParser() {
		this.messageParser = new MessageParser(config);
	}

	public Event buildEvent(Topic topic, ConsumerRecord<byte[], byte[]> record) {
		
		byte[] message = record.value();
		
		JsonNode eventJson = messageParser.parseMessage(message);
		log.debug("Parsed eventJson: " + eventJson.toString());
		
		boolean messageIsAvro = messageParser.isMessageAvro(message);
		if(messageIsAvro) {
			return new Event(config, message, topic, eventJson, messageIsAvro, messageParser.getAvroSchemaHashForMessage(message), messageParser.getSchemaForMessage(message));
		} else {
			return new Event(config, message, topic, eventJson, messageIsAvro, null, null);
		}
	}
}
