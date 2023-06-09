package de.esailors.dataheart.drillviews.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

public class Topic implements Comparable<Topic> {

	private static final Logger log = LogManager.getLogger(Topic.class.getName());

	private String name;
	private int partitionCount = -1;

	private Set<Event> events = new HashSet<>();
	private Set<Event> invalidEvents = new HashSet<>();
	private List<String> reportMessages = new ArrayList<>();
	private List<byte[]> brokenMessages = new ArrayList<>();

	private Map<String, Set<Event>> eventTypeNames;

	public Topic(String topicName) {
		this.name = topicName;
	}

	public void markInconsistencies() {
		eventTypeNames = new HashMap<>();

		boolean sawEventsWithoutEventType = false;
		boolean sawEventsWithoutTimestamp = false;
		for (Event event : events) {
			Optional<String> readEventTypeOption = event.readEventType();
			if (!readEventTypeOption.isPresent()) {
				invalidEvents.add(event);
				sawEventsWithoutEventType = true;
				continue;
			}

			Optional<String> readTimestampOption = event.readTimestamp();
			if (!readTimestampOption.isPresent()) {
				invalidEvents.add(event);
				sawEventsWithoutTimestamp = true;
				continue;
			}

			addEventForEventType(readEventTypeOption.get(), event);
		}
		if (invalidEvents.size() > 0) {
			addMessageToReport("Invalid events detected in: " + this);
		}
		if (sawEventsWithoutEventType) {
			addMessageToReport("Event without eventType detected in: " + this);
		}
		if (sawEventsWithoutTimestamp) {
			addMessageToReport("Event without timestamp detected in: " + this);
		}
		if (eventTypeNames.size() > 1) {
			addMessageToReport("Mixed EventTypes within the same topic: " + this);
		}
	}

	private void addEventForEventType(String eventTypeName, Event event) {
		if (eventTypeNames.get(eventTypeName) == null) {
			eventTypeNames.put(eventTypeName, new HashSet<>());
		}
		eventTypeNames.get(eventTypeName).add(event);
	}

	public boolean isConsistent() {
		if (eventTypeNames == null) {
			throw new IllegalStateException("Can't tell if topic is conistent yet, call markInconsistencies() first");
		}
		return eventTypeNames.size() == 1 && invalidEvents.size() == 0 && brokenMessages.size() == 0;
	}

	public void addEvent(Event event) {
		events.add(event);
	}

	public Set<Event> getEvents() {
		return events;
	}

	public Set<Event> getInvalidEvents() {
		return invalidEvents;
	}

	public String getName() {
		return name;
	}

	public int getPartitionCount() {
		return partitionCount;
	}

	public Map<String, Set<Event>> getEventTypeNames() {
		return eventTypeNames;
	}

	public void setPartitionCount(int partitionCount) {
		this.partitionCount = partitionCount;
	}

	private void addMessageToReport(String message) {
		log.warn("Topic report message: " + message);
		reportMessages.add(message);
	}

	public List<String> getReportMessages() {
		return reportMessages;
	}

	public void addBrokenMessage(byte[] message) {
		brokenMessages.add(message);
		if (message != null) {
			addMessageToReport("Received a broken message: " + new String(message));
		} else {
			addMessageToReport("Received a null message");
		}
		
	}
	
	@Override
	public String toString() {
		return "Topic [name=" + name + "]";
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
		Topic other = (Topic) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	@Override
	public int compareTo(Topic o) {
		return name.compareTo(o.name);
	}

}
