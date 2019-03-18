package de.esailors.dataheart.drillviews.data;

import java.util.Set;

public class EventStructure {

	private EventType eventType;
	private String structureBaseName;
	private Tree eventStructureTree;

	private EventStructureSource source;

	public EventStructure(String structureBaseName, Event sourceEvent, EventType eventType) {
		this.eventType = eventType;
		this.structureBaseName = structureBaseName;
		this.source = new EventStructureSource(sourceEvent);
		this.eventStructureTree = TreeFactory.getInstance().buildTreeFromJsonNode(sourceEvent.getEventJson(),
				structureBaseName);
	}

	public EventStructure(AvroSchema avroSchema, String structureBaseName) {
		this.eventType = avroSchema.getEventType();
		this.structureBaseName = structureBaseName;
		this.source = new EventStructureSource(avroSchema);
		this.eventStructureTree = TreeFactory.getInstance().buildTreeFromAvroSchema(avroSchema.getSchema());
	}

	public EventStructure(String structureBaseName, Set<EventStructure> eventStructures) {
		this.structureBaseName = structureBaseName;
		this.source = new EventStructureSource(eventStructures);
		this.eventStructureTree = EventStructureMerger.getInstance().mergeEventStructures(eventStructures);

		this.eventType = collectEventTypeFrom(eventStructures);
	}

	private EventType collectEventTypeFrom(Set<EventStructure> eventStructures) {
		EventType r = null;
		for (EventStructure eventStructure : eventStructures) {
			if (r == null) {
				r = eventStructure.getEventType();
			} else {
				if (!r.equals(eventStructure.getEventType())) {
					throw new IllegalArgumentException("Received EventStructures with different eventTypes: " + r
							+ " and " + eventStructure.getEventType());
				}
			}
		}
		return r;
	}

	public EventType getEventType() {
		return eventType;
	}

	public String getStructureBaseName() {
		return structureBaseName;
	}

	public EventStructureSource getSource() {
		return source;
	}

	public Tree getEventStructureTree() {
		return eventStructureTree;
	}

	@Override
	public String toString() {
		return structureBaseName + "_" + source.toString() + "_" + hashCode();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((eventStructureTree == null) ? 0 : eventStructureTree.hashCode());
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
		return true;
	}

	public String toDot() {
		return eventStructureTree.toDot();
	}

}
