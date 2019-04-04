package de.esailors.dataheart.drillviews.data;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonNode;

import com.google.common.base.Optional;

import de.esailors.dataheart.drillviews.conf.Config;

public class Event {
	
	private static final int TIMESTAMP_LENGTH_MILLISECONDS_THRESHOLD = 12;

	private static final Logger log = LogManager.getLogger(Event.class.getName());
	
	private byte[] message;
	private Topic topic;
	private JsonNode eventJson;
	private boolean isAvroMessage;
	private Schema schema;
	private String avroSchemaHash;

	public Event(byte[] message, Topic topic, JsonNode eventJson, boolean isAvroMessage, String avroSchemaHash, Schema schema) {
		this.message = message; 
		this.topic = topic;
		this.eventJson = eventJson;
		this.isAvroMessage = isAvroMessage;
		this.schema = schema;
		this.avroSchemaHash = avroSchemaHash;
	}

	public Optional<TimestampType> determineTimestampType() {
		Optional<String> readTimestamp = readTimestamp();
		if(!readTimestamp.isPresent()) {
			return Optional.absent();
		}
		if(readTimestamp.get().length() < TIMESTAMP_LENGTH_MILLISECONDS_THRESHOLD) {
			return Optional.of(TimestampType.SECONDS);
		} else {
			return Optional.of(TimestampType.MILLISECONDS);
		}
	}
	
	public Optional<String> readId() {
		return fieldFromEvent(Config.getInstance().EVENT_FIELD_ID);
	}
	
	public Optional<String> readTimestamp() {
		return fieldFromEvent(Config.getInstance().EVENT_FIELD_TIMESTAMP);
	}
	
	public Optional<String> readEventType() {
		return fieldFromEvent(Config.getInstance().EVENT_FIELD_EVENT_TYPE);
	}

	public Optional<String> readSchemaVersion() {
		return fieldFromEvent(Config.getInstance().EVENT_FIELD_VERSION);
	}

	public Optional<String> fieldFromEvent(String field) {
		JsonNode jsonNode = getEventJson().get(field);
		if (jsonNode == null) {
			log.warn("Field not found for '" + field + "' in: " + this);
			return Optional.absent();
		}
		return Optional.of(jsonNode.asText());
	}

	public byte[] getMessage() {
		return message;
	}

	public Topic getTopic() {
		return topic;
	}

	public JsonNode getEventJson() {
		return eventJson;
	}

	public boolean isAvroMessage() {
		return isAvroMessage;
	}

	public Schema getSchema() {
		return schema;
	}
	
	public String getAvroSchemaHash() {
		return avroSchemaHash;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(message);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Event other = (Event) obj;
		if (!Arrays.equals(message, other.message))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Event [topic=" + topic + "]";
	}

}
