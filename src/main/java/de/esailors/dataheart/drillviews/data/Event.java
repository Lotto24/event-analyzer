package de.esailors.dataheart.drillviews.data;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;

public class Event {
	
	private byte[] message;
	private Topic topic;
	private JsonNode eventJson;
	private boolean messageIsAvro;
	private Schema schema;

	public Event(byte[] message, Topic topic, JsonNode eventJson, boolean messageIsAvro, Schema schema) {
		this.message = message; 
		this.topic = topic;
		this.eventJson = eventJson;
		this.messageIsAvro = messageIsAvro;
		this.schema = schema;
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

	public boolean isMessageIsAvro() {
		return messageIsAvro;
	}

	public Schema getSchema() {
		return schema;
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

}
