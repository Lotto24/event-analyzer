package de.esailors.dataheart.drillviews.data;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonNode;

import de.esailors.dataheart.drillviews.conf.Config;

public class Event {
	
	private static final Logger log = LogManager.getLogger(Event.class.getName());
	
	private Config config;
	
	private byte[] message;
	private Topic topic;
	private JsonNode eventJson;
	private boolean avroMessage;
	private Schema avroSchema;
	private String avroSchemaHash;

	public Event(Config config, byte[] message, Topic topic, JsonNode eventJson, boolean avroMessage, String avroSchemaHash, Schema avroSchema) {
		this.config = config;
		
		this.message = message; 
		this.topic = topic;
		this.eventJson = eventJson;
		this.avroMessage = avroMessage;
		this.avroSchema = avroSchema;
		this.avroSchemaHash = avroSchemaHash;
	}
	
	
	public String readId() {
		return fieldFromEvent(config.EVENT_FIELD_ID);
	}
	
	public String readTimestamp() {
		return fieldFromEvent(config.EVENT_FIELD_TIMESTAMP);
	}
	
	public String readEventType() {
		return fieldFromEvent(config.EVENT_FIELD_EVENT_TYPE);
	}

	public String readSchemaVersion() {
		return fieldFromEvent(config.EVENT_FIELD_VERSION);
	}

	public String fieldFromEvent(String field) {
		JsonNode jsonNode = getEventJson().get(field);
		if (jsonNode == null) {
			log.warn("Field not found for '" + field + "' in: " + this);
			return null;
		}
		return jsonNode.asText();
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
		return avroMessage;
	}

	public Schema getAvroSchema() {
		return avroSchema;
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
