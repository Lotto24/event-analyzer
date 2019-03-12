package de.esailors.dataheart.drillviews.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Topic {

	private static final Logger log = LogManager.getLogger(Topic.class.getName());

	private String topicName;
	private int partitionCount = -1;

	private Set<Event> events = new HashSet<>();
	private Event exampleEvent;
	
	private Set<EventStructure> eventStructures;
	private Set<Boolean> messagesAreAvro;
	private Set<String> eventTypes;
	private Set<String> schemaVersions;
	private Map<String, Schema> avroSchemas;

	private List<String> reportMessages;
	
	public Topic(String topicName) {
		this.topicName = topicName;
		
		// TODO get more configuration information from topic (can we get retention?)
		// TODO somehow link to official eSailors/kafka-events repo
		// https://srv-git-01-hh1.alinghi.tipp24.net/eSailors/kafka-events
	}

	public void markInconsistencies() {
		// TODO this has gotten pretty big, probably a good idea to move this out of this class by now
		
		// check consistency within topics
		// - each topic only has avro / json - if avro always the same schema?
		// - check JSON structure doesnt change (always the same event structure)
		// - always the same event Type in each topic

		reportMessages = new ArrayList<>();
		
		if (getEvents().size() < 2) {
			addMessageToReport("Can't properly check event consistency because I did not get enough events for: " + this + " (received " + getEvents().size() + ")");
		}

		// idea: gather values in Sets, if they have more than 1 entry afterwards
		// something is fishy
		eventStructures = new HashSet<>();
		eventTypes = new HashSet<>();
		schemaVersions = new HashSet<>();
		messagesAreAvro = new HashSet<>();
		avroSchemas = new HashMap<>();

		Event firstEvent = null;
		Iterator<Event> iterator = getEvents().iterator();
		while (iterator.hasNext()) {
			Event event = iterator.next();

			eventStructures.add(new EventStructure(this.topicName, event));
			
			messagesAreAvro.add(event.isAvroMessage());
			avroSchemas.put(event.getAvroSchemaHash(), event.getAvroSchema());
			eventTypes.add(event.readEventType());
			schemaVersions.add(event.readSchemaVersion());
			if(avroSchemas.get(event.getAvroSchemaHash()) != null && !avroSchemas.get(event.getAvroSchemaHash()).equals(event.getAvroSchema())) {
				addMessageToReport("Found two different Avro schemas with the same hash: " + event.getAvroSchemaHash());
			}

			if (firstEvent == null) {
				// first one we see, use this as example event
				firstEvent = event;
				setExampleEvent(event);
			}

		}

		if (eventStructures.size() > 1) {
			addMessageToReport("Mixed EventStructures within the same topic: " + this);
		}
		if (eventTypes.size() > 1) {
			addMessageToReport("Mixed EventTypes within the same topic: " + this);
		}
		if (schemaVersions.size() > 1) {
			addMessageToReport("Mixed Schema Versions within the same topic: " + this);
		}
		if (messagesAreAvro.size() > 1) {
			addMessageToReport("Mixed Avro and plain JSON within the same topic: " + this);
		}
		if (avroSchemas.size() > 1) {
			addMessageToReport("Mixed Avro schemas within the same topic: " + this);
		}

	}

	private void addMessageToReport(String message) {
		log.warn("Topic report message: " + message);
		reportMessages.add(message);
	}

	public boolean isConsistent() {
		if (eventStructures == null || messagesAreAvro == null || avroSchemas == null || eventTypes == null || schemaVersions == null) {
			throw new IllegalStateException("Can't tell if topic is conistent yet, call markInconsistencies() first");
		}
		return eventStructures.size() == 1 && messagesAreAvro.size() == 1 && avroSchemas.keySet().size() == 1 && eventTypes.size() == 1
				&& schemaVersions.size() == 1;
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

	public Event getExampleEvent() {
		return exampleEvent;
	}

	public Set<Event> getEvents() {
		return events;
	}

	public String getName() {
		return topicName;
	}

	public int getPartitionCount() {
		return partitionCount;
	}

	public Set<Boolean> getMessagesAreAvro() {
		return messagesAreAvro;
	}

	public Set<String> getEventTypes() {
		return eventTypes;
	}
	
	public Set<String> getSchemaVersions() {
		return schemaVersions;
	}
	
	public Set<EventStructure> getEventStructures() {
		return eventStructures;
	}

	public Map<String, Schema> getAvroSchemas() {
		return avroSchemas;
	}
	
	public Set<String> getAvroSchemaHashes() {
		return avroSchemas.keySet();
	}
	
	public Collection<Schema> getMessageSchemas() {
		return avroSchemas.values();
	}

	public void setPartitionCount(int partitionCount) {
		this.partitionCount = partitionCount;
	}
	
	public List<String> getReportMessages() {
		return reportMessages;
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
