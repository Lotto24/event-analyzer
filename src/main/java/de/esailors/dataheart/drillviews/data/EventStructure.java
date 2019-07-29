package de.esailors.dataheart.drillviews.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import java.util.Optional;

import de.esailors.dataheart.drillviews.data.EventStructureSource.Type;

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
		List<EventStructure> sourceEventStructures = new ArrayList<>(eventType.getEventStructures());
		if (eventType.getAvroSchemas() != null) {
			for (AvroSchema avroSchema : eventType.getAvroSchemas().values()) {
				sourceEventStructures.add(avroSchema.getEventStructure());
			}
		}
		Optional<EventStructure> deserializedEventStructureOption = eventType.getDeserializedEventStructureOption();
		if (deserializedEventStructureOption.isPresent()) {
			sourceEventStructures.add(deserializedEventStructureOption.get());
		}
		this.source = new EventStructureSource(sourceEventStructures);
		this.eventType = eventType;
		this.eventStructureTree = EventStructureMerger.getInstance().mergeEventStructures(eventType,
				sourceEventStructures);
	}

	public EventStructure(EventType eventType, Tree deserializedTree) {
		this.source = new EventStructureSource();
		this.eventType = eventType;
		this.eventStructureTree = deserializedTree;
	}

	public Set<String> getSourceStructureNames() {
		// read source structures from root node properties to not miss structures from
		// deserialized older sources
		return getEventStructureTree().getRootNode().getProperty(NodePropertyType.SOURCE_STRUCTURE);
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

	@Override
	public String toString() {
		Type sourceType = source.getType();
		switch (sourceType) {
		case MERGE: {
			return sourceType.toString() + "_" + eventType.getName();
		}
		case AVRO: {
			Optional<AvroSchema> sourceSchemaOption = source.getSourceSchema();
			if (!sourceSchemaOption.isPresent()) {
				throw new IllegalStateException("Expect EventStructure of type AVRO to have a sourceSchema present");
			}
			AvroSchema sourceSchema = sourceSchemaOption.get();
			String schemaVersion = sourceSchema.getSchemaVersion().isPresent() ? sourceSchema.getSchemaVersion().get()
					: "?";
			return sourceType.toString() + "_" + eventType.getName() + "_" + schemaVersion + "_"
					+ sourceSchema.getSchemaHash();
		}
		default: {
			return source.toString() + "_" + hashCode();
		}
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((eventStructureTree == null) ? 0 : eventStructureTree.hashCode());
		result = prime * result + ((eventType == null) ? 0 : eventType.hashCode());
		// we don't want to use the whole EventStructureSource, mostly because we try to
		// avoid defining equality for this. but the source type should be taken into
		// account, as we still want to merge properties between JSON and Avro
		// structures. Enums hashCode however is not stable (delegates to
		// Object.hashCode() which is memory address dependent). but we want this
		// hashCode to be stable in the sense that different runs will give the same
		// hashCode for the same abstract event structure and source. thus we take the
		// string representation of the Enum here
		result = prime * result + ((source == null) ? 0 : source.getType().toString().hashCode());
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
