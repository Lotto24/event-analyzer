package de.esailors.dataheart.drillviews.data;

import java.util.HashSet;
import java.util.Set;

import org.apache.avro.Schema;

public class Topic {

	private String topicName;
	private int partitionCount = -1;

	private Set<Event> events = new HashSet<>();
	private Event exampleEvent;
	private Set<Boolean> messagesAreAvro;
	private Set<Schema> messageSchemas;
	private Set<String> eventTypes;

	public Topic(String topicName) {
		this.topicName = topicName;
	}

	public void addEvent(Event event) {
		events.add(event);
	}
	
	public void setExampleEvent(Event exampleEvent) {
		if (!events.contains(exampleEvent)) {
			throw new IllegalArgumentException("Was given an example event that I don't know " + this);
		}
		this.exampleEvent = exampleEvent;
	}
	
	public boolean isConsistent() {
		return messagesAreAvro.size() == 1 && messageSchemas.size() == 1 && eventTypes.size() == 1;
	}
	
	public void setMessagesAreAvro(Set<Boolean> messagesAreAvro) {
		this.messagesAreAvro = messagesAreAvro;
	}

	public void setMessageSchemas(Set<Schema> messageSchemas) {
		this.messageSchemas = messageSchemas;
	}

	public void setEventTypes(Set<String> eventTypes) {
		this.eventTypes = eventTypes;
	}

	public Event getExampleEvent() {
		return exampleEvent;
	}

	public Set<Event> getEvents() {
		return events;
	}

	public String getTopicName() {
		return topicName;
	}

	public int getPartitionCount() {
		return partitionCount;
	}

	public Set<Boolean> getMessagesAreAvro() {
		return messagesAreAvro;
	}

	public Set<Schema> getMessageSchemas() {
		return messageSchemas;
	}

	public Set<String> getEventTypes() {
		return eventTypes;
	}

	public void setPartitionCount(int partitionCount) {
		this.partitionCount = partitionCount;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((topicName == null) ? 0 : topicName.hashCode());
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
		Topic other = (Topic) obj;
		if (topicName == null) {
			if (other.topicName != null)
				return false;
		} else if (!topicName.equals(other.topicName))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Topic '" + topicName + "'";
	}

}
