package de.esailors.dataheart.drillviews.data;

import java.util.Set;

import org.apache.avro.Schema;

public class EventStructure {

	private String eventType;
	private String structureBaseName;
	private Tree eventStructureTree;

	private EventStructureSource source;

	public EventStructure(String structureBaseName, Event sourceEvent) {
		this.eventType = sourceEvent.readEventType(); // TODO handle eventType not existing / invalid events in general
		this.structureBaseName = structureBaseName;
		this.source = new EventStructureSource(sourceEvent);
		this.eventStructureTree = TreeFactory.getInstance().buildTreeFromJsonNode(sourceEvent.getEventJson(),
				structureBaseName);
	}

	public EventStructure(String eventType, String structureBaseName, Schema sourceSchema, String sourceSchemaHash) {
		this.eventType = eventType;
		this.structureBaseName = structureBaseName;
		this.source = new EventStructureSource(sourceSchema, sourceSchemaHash);
		this.eventStructureTree = TreeFactory.getInstance().buildTreeFromAvroSchema(sourceSchema);

		// TODO not used yet
	}

	public EventStructure(String structureBaseName, Set<EventStructure> eventStructures) {
		this.structureBaseName = structureBaseName;
		this.source = new EventStructureSource(eventStructures);
		this.eventStructureTree = EventStructureMerger.getInstance().mergeEventStructures(eventStructures);

		this.eventType = collectEventTypeFrom(eventStructures);
	}

	private String collectEventTypeFrom(Set<EventStructure> eventStructures) {
		String r = null;
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

	public String getEventType() {
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
