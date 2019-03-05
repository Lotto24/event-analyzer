package de.esailors.dataheart.drillviews.data;

import java.util.HashSet;
import java.util.Set;

public class Topic {

	private String topicName;
	private int partitionCount = -1;
	
	private Set<Event> events = new HashSet<>();

	public Topic(String topicName) {
		this.topicName = topicName;
	}
	
	public void addEvent(Event event) {
		events.add(event);
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

	public void setPartitionCount(int partitionCount) {
		this.partitionCount  = partitionCount;
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
