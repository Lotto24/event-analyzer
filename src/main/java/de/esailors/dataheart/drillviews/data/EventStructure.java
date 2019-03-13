package de.esailors.dataheart.drillviews.data;

import java.util.Set;

import org.apache.avro.Schema;

public class EventStructure {

	private String structureBaseName;
	private Tree eventStructureTree;

	private EventStructureSource source;
	
	public EventStructure(String structureBaseName, Event sourceEvent) {
		this.structureBaseName = structureBaseName;
		this.source = new EventStructureSource(sourceEvent);
		this.eventStructureTree = TreeFactory.getInstance().buildTreeFromJsonNode(sourceEvent.getEventJson(), structureBaseName);
	}
	
	public EventStructure(String structureBaseName, Schema sourceSchema, String sourceSchemaHash) {
		this.structureBaseName = structureBaseName;
		this.source = new EventStructureSource(sourceSchema, sourceSchemaHash);
		this.eventStructureTree = TreeFactory.getInstance().buildTreeFromAvroSchema(sourceSchema);
		
		// TODO not used yet
	}
	
	public EventStructure(String structureBaseName, Set<EventStructure> eventStructures) {
		this.structureBaseName = structureBaseName;
		this.source = new EventStructureSource(eventStructures);
		this.eventStructureTree = EventStructureMerger.getInstance().mergeEventStructures(eventStructures);
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
