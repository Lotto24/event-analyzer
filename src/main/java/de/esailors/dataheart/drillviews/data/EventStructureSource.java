package de.esailors.dataheart.drillviews.data;

import java.util.Collection;
import java.util.List;

import java.util.Optional;

public class EventStructureSource {

	private static final int MERGED_STRUCTURE_NAME_MAX_SOURCE_HASHCODES = 5;

	public enum Type {
		EVENT, AVRO, MERGE, DESERIALIZED
	}

	private Type type;

	private Event sourceEvent; // for structures generated from json events
	private List<EventStructure> sourceStructures; // for structures generated by merges
	private AvroSchema sourceSchema; // for structures generated from avro schemas

	public EventStructureSource(Event sourceEvent) {
		type = Type.EVENT;
		this.sourceEvent = sourceEvent;
	}

	public EventStructureSource(AvroSchema sourceSchema) {
		type = Type.AVRO;
		this.sourceSchema = sourceSchema;
	}

	public EventStructureSource(List<EventStructure> sourceStructures) {
		type = Type.MERGE;
		this.sourceStructures = sourceStructures;
	}
	
	public EventStructureSource() {
		type = Type.DESERIALIZED;
	}

	public Type getType() {
		return type;
	}

	public Optional<Event> getSourceEvent() {
		return Optional.ofNullable(sourceEvent);
	}

	public Optional<AvroSchema> getSourceSchema() {
		return Optional.ofNullable(sourceSchema);
	}

	public Optional<Collection<EventStructure>> getSourceStructures() {
		return Optional.ofNullable(sourceStructures);
	}

	@Override
	public String toString() {
		String r = getType().toString();
		switch (type) {
		case EVENT: {
			Optional<String> readEventType = sourceEvent.readEventType();
			Optional<String> readSchemaVersion = sourceEvent.readSchemaVersion();
			r += "_" + (readEventType.isPresent() ? readEventType.get() : "UNKNOWN_EVENT_TYPE") + "_" + (readSchemaVersion.isPresent() ? readSchemaVersion.get() : "UNKNOWN_SCHEMA_VERSION");
			break;
		}
		case AVRO: {
			Optional<String> schemaVersionOption = sourceSchema.getSchemaVersion();
			r += "_" + sourceSchema.getSchema().getName() + "_" + (schemaVersionOption.isPresent() ? schemaVersionOption.get() : "UNKNOWN_SCHEMA_VERSION");
			break;
		}
		case MERGE: {
			r += "_" + sourceStructures.size();
			// when there are a lot of different sourceStructures the returned name
			// gets really long. as this is used as the filename of the rendered .dot plot
			// this is causing issues. so we only take the first 5 source structures into account
			int cnt = 0;
			for (EventStructure sourceStructure : sourceStructures) {
				r += "_" + sourceStructure.hashCode();
				cnt++;
				
				if(cnt >= MERGED_STRUCTURE_NAME_MAX_SOURCE_HASHCODES) {
					r += "_CROPPED";
					break;
				}
			}
			break;
		}
		case DESERIALIZED: {
			break;
		}
		default: throw new IllegalStateException("Unexpected EventStructureSource.Type: " + type);
		}
		return r;
	}

}
