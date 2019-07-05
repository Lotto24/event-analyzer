package de.esailors.dataheart.drillviews.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;

public class EventType implements Comparable<EventType> {

	private static final Logger log = LogManager.getLogger(EventType.class.getName());

	private Event exampleEvent;

	private String name;
	private List<Topic> sourceTopics = new ArrayList<>();
	private Set<Event> events = new HashSet<>();

	private Optional<EventStructure> deserializedEventStructureOption = Optional.absent();
	
	// absent if there are no events
	private Optional<EventStructure> mergedEventStructure;

	private Set<EventStructure> eventStructures;

	private Set<Optional<String>> schemaVersions;
	private Map<String, AvroSchema> avroSchemas;
	private Set<Boolean> messagesAreAvro;
	private Set<TimestampType> timestampTypes;

	private List<String> reportMessages = new ArrayList<>();

	private Optional<Long> drillViewCountOption = Optional.absent();
	private Optional<Long> hiveViewCountOption = Optional.absent();


	public EventType(String eventTypeName, Topic sourceTopic, Set<Event> events) {
		this.name = eventTypeName;
		addSourceTopic(sourceTopic);
		addEvents(events);
	}

	public void markInconsistencies() {
		// check consistency within topics
		// - each topic only has avro / json - if avro always the same schema?
		// - check JSON structure doesnt change (always the same event structure)
		// - always the same event Type in each topic
		// - timestamps always milliseconds, not in seconds

		if (getEvents().size() < 2) {
			addMessageToReport("Can't properly check event consistency because I did not get enough events for: " + this
					+ " (received " + getEvents().size() + ")");
		}

		// idea: gather values in Sets, if they have more than 1 entry afterwards
		// something is fishy
		eventStructures = new HashSet<>();
		schemaVersions = new HashSet<>();
		messagesAreAvro = new HashSet<>();
		avroSchemas = new HashMap<>();
		timestampTypes = new HashSet<>();

		Event firstEvent = null;
		Iterator<Event> iterator = getEvents().iterator();
		while (iterator.hasNext()) {
			Event event = iterator.next();

			eventStructures.add(new EventStructure(event, this));

			Optional<String> schemaVersionOption = event.readSchemaVersion();
			schemaVersions.add(schemaVersionOption);

			Optional<TimestampType> timestampTypeOption = event.determineTimestampType();
			if (!timestampTypeOption.isPresent()) {
				throw new IllegalStateException("Detected event without a timestamp in (" + this
						+ "), expect all invalid events to be filtered out on Topic level already: "
						+ new String(event.getMessage()));
			}
			timestampTypes.add(timestampTypeOption.get());

			boolean isAvroMessage = event.isAvroMessage();
			messagesAreAvro.add(isAvroMessage);
			if (isAvroMessage) {
				AvroSchema avroSchema = new AvroSchema(event.getAvroSchemaHash(), event.getSchema(),
						schemaVersionOption, this);
				avroSchemas.put(event.getAvroSchemaHash(), avroSchema);
			}

			if (firstEvent == null) {
				// first one we see, use this as example event
				firstEvent = event;
				setExampleEvent(event);
			}

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
		if (timestampTypes.size() > 1) {
			addMessageToReport("Mixed timestamp types within the same topic: " + this);
		}
		if (timestampTypes.contains(TimestampType.SECONDS)) {
			addMessageToReport("Timestamp in seconds instead of milliseconds detected in: " + this);
		}

	}

	public boolean isConsistent() {
		if (eventStructures == null || messagesAreAvro == null || avroSchemas == null || schemaVersions == null
				|| timestampTypes == null) {
			throw new IllegalStateException("Can't tell if topic is conistent yet, call markInconsistencies() first");
		}
		return messagesAreAvro.size() == 1 && avroSchemas.keySet().size() <= 1 && schemaVersions.size() == 1
				&& timestampTypes.size() == 1 && !timestampTypes.contains(TimestampType.SECONDS);
	}

	public void setDeserializedEventStructure(EventStructure deserializedEventStructure) {
		// for deserialized event structures
		deserializedEventStructureOption  = Optional.of(deserializedEventStructure);
	}
	
	public Optional<EventStructure> getDeserializedEventStructureOption() {
		return deserializedEventStructureOption;
	}

	public void buildMergedEventStructure() {
		if (eventStructures == null) {
			throw new IllegalStateException(
					"Can't build combined event structure yet, call markInconsistencies() first");
		}
		if (eventStructures.isEmpty()) {
			// could theoretically still build merged event structure from avro schema
			// potentially, but in practice we did not fetch a schema without an event
			log.warn("Unable to build merged even structure due to missing events for " + this);
			mergedEventStructure = Optional.absent();
		} else {
			mergedEventStructure = Optional.of(new EventStructure(this));
		}
	}

	public Optional<EventStructure> getMergedEventStructured() {
		if (mergedEventStructure == null || !mergedEventStructure.isPresent()) {
			throw new IllegalStateException(
					"Can't provide combined event structures yet, call buildMergedEventStructure() first");
		}
		return mergedEventStructure;
	}

	public void addEvents(Set<Event> events) {
		for (Event event : events) {
			addEvent(event);
		}
	}

	public void addEvent(Event event) {
		Optional<String> eventTypeOption = event.readEventType();
		if (!eventTypeOption.isPresent()) {
			throw new IllegalArgumentException("Received an event without an event type for: " + this);
		}
		String eventType = eventTypeOption.get();
		if (!eventType.equals(name)) {
			throw new IllegalArgumentException(
					"Received an event that does not share my event type name: event: " + eventType + ", me: " + this);
		}

		events.add(event);
	}

	public Set<Event> getEvents() {
		return events;
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

	public Set<Boolean> getMessagesAreAvro() {
		return messagesAreAvro;
	}

	public Set<Optional<String>> getSchemaVersions() {
		return schemaVersions;
	}

	public Set<EventStructure> getEventStructures() {
		return eventStructures;
	}

	public Map<String, AvroSchema> getAvroSchemas() {
		return avroSchemas;
	}

	public Set<TimestampType> getTimestampTypes() {
		return timestampTypes;
	}

	public Set<String> getAvroSchemaHashes() {
		if (avroSchemas == null) {
			return null;
		}
		return avroSchemas.keySet();
	}

	public Collection<AvroSchema> getMessageSchemas() {
		if (avroSchemas == null) {
			return null;
		}
		return avroSchemas.values();
	}

	public void addSourceTopic(Topic sourceTopic) {
		sourceTopics.add(sourceTopic);
	}

	public List<Topic> getSourceTopics() {
		return sourceTopics;
	}

	private void addMessageToReport(String message) {
		log.warn(this + " report message: " + message);
		reportMessages.add(message);
	}

	public List<String> getReportMessages() {
		return reportMessages;
	}

	public String getName() {
		return name;
	}

	public Optional<Long> getDrillViewCountOption() {
		return drillViewCountOption;
	}

	public void setDrillViewCount(long drillViewCount) {
		drillViewCountOption = Optional.of(drillViewCount);
	}
	
	public Optional<Long> getHiveViewCountOption() {
		return hiveViewCountOption;
	}

	public void setHiveViewCount(long hiveViewCount) {
		hiveViewCountOption = Optional.of(hiveViewCount);
	}

	@Override
	public String toString() {
		return "EventType [name=" + name + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		EventType other = (EventType) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	@Override
	public int compareTo(EventType o) {
		return name.compareTo(o.name);
	}

}
