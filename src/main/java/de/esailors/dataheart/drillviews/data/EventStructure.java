package de.esailors.dataheart.drillviews.data;

public class EventStructure {

	private String structureBaseName;
	private Event structureSource;
	private Tree eventStructureTree;
	
	public EventStructure(String structureBaseName, Event structureSource) {
		this.structureBaseName = structureBaseName;
		this.structureSource = structureSource;
		this.eventStructureTree = TreeFactory.getInstance().buildTreeFromJsonNode(structureSource.getEventJson(), structureBaseName);
		
		// TODO handle structure of avro schema as well
	}
	
	public String structureSpecificName() {
		return structureBaseName + "_" + structureSource.readSchemaVersion() + "_" + hashCode();
	}
	
	public String getStructureBaseName() {
		return structureBaseName;
	}

	public Event getStructureSource() {
		return structureSource;
	}
	
	public Tree getEventStructureTree() {
		return eventStructureTree;
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
