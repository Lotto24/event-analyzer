package de.esailors.dataheart.drillviews.processor;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.codehaus.jackson.JsonNode;

public class Event {
	
	private byte[] message;
	private String topic;
	private JsonNode eventJson;
	private boolean messageIsAvro;
	private Schema schema;

	public Event(byte[] message, String topic, JsonNode eventJson, boolean messageIsAvro, Schema schema) {
		this.message = message; 
		this.topic = topic;
		this.eventJson = eventJson;
		this.messageIsAvro = messageIsAvro;
		this.schema = schema;
	}

	public byte[] getMessage() {
		return message;
	}

	public String getTopic() {
		return topic;
	}

	public JsonNode getEventJson() {
		return eventJson;
	}

	public boolean isMessageIsAvro() {
		return messageIsAvro;
	}

	public Schema getSchema() {
		return schema;
	}

}
