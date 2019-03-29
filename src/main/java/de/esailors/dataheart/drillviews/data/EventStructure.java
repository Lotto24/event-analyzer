package de.esailors.dataheart.drillviews.data;

import java.util.HashSet;
import java.util.Set;

public class EventStructure {

	private EventType eventType;
	private Tree eventStructureTree;

	private EventStructureSource source;

	public EventStructure(Event sourceEvent, EventType eventType) {
		this.source = new EventStructureSource(sourceEvent);
		this.eventType = eventType;
		this.eventStructureTree = TreeFactory.getInstance().buildTreeFromJsonNode(sourceEvent.getEventJson(),
				eventType.getName());
	}

	public EventStructure(AvroSchema avroSchema) {
		this.source = new EventStructureSource(avroSchema);
		this.eventType = avroSchema.getEventType();
		this.eventStructureTree = TreeFactory.getInstance().buildTreeFromAvroSchema(avroSchema.getSchema());
	}

	public EventStructure(EventType eventType) {
		// merged event structure
		Set<EventStructure> sourceEventStructures = new HashSet<>(eventType.getEventStructures());
		if (eventType.getAvroSchemas() != null) {
			for (AvroSchema avroSchema : eventType.getAvroSchemas().values()) {
				sourceEventStructures.add(avroSchema.getEventStructure());
			}
		}
		this.source = new EventStructureSource(sourceEventStructures);
		this.eventType = eventType;
		this.eventStructureTree = EventStructureMerger.getInstance().mergeEventStructures(eventType,
				sourceEventStructures);
	}

	public EventType getEventType() {
		return eventType;
	}

	public EventStructureSource getSource() {
		return source;
	}

	public Tree getEventStructureTree() {
		return eventStructureTree;
	}

	public String toDot() {
		return eventStructureTree.toDot();
	}

	@Override
	public String toString() {
		return source.toString() + "_" + hashCode();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((eventStructureTree == null) ? 0 : eventStructureTree.hashCode());
		result = prime * result + ((eventType == null) ? 0 : eventType.hashCode());
		result = prime * result + ((source == null) ? 0 : source.getType().hashCode());
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
		EventStructure other = (EventStructure) obj;
		if (eventStructureTree == null) {
			if (other.eventStructureTree != null)
				return false;
		} else if (!eventStructureTree.equals(other.eventStructureTree))
			return false;
		if (eventType == null) {
			if (other.eventType != null)
				return false;
		} else if (!eventType.equals(other.eventType))
			return false;
		if (source == null) {
			if (other.source != null)
				return false;
		} else if (!source.getType().equals(other.source.getType()))
			return false;
		return true;
	}

}
