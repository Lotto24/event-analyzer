package de.esailors.dataheart.drillviews.kafka;

import java.io.IOException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonNode;

import de.esailors.dataheart.drillviews.data.Event;
import de.esailors.dataheart.drillviews.data.Topic;
import de.esailors.dataheart.drillviews.exception.UnknownSchemaException;

public class EventFactory {

	private static final Logger log = LogManager.getLogger(EventFactory.class.getName());

	private MessageParser messageParser;

	public EventFactory() {
		this.messageParser = new MessageParser();
	}

	public Event buildEvent(Topic topic, ConsumerRecord<byte[], byte[]> record) throws IOException, UnknownSchemaException {

		byte[] message = record.value();

		JsonNode eventJson = messageParser.parseMessage(message);
		log.debug("Parsed eventJson: " + eventJson.toString());

		boolean messageIsAvro = messageParser.isMessageAvro(message);
		if (messageIsAvro) {
			return new Event(message, topic, eventJson, messageIsAvro,
					messageParser.getAvroSchemaHashForMessage(message), messageParser.getSchemaForMessage(message));
		} else {
			return new Event(message, topic, eventJson, messageIsAvro, null, null);
		}
	}
}
