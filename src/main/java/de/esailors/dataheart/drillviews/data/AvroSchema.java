package de.esailors.dataheart.drillviews.data;

import org.apache.avro.Schema;

import com.google.common.base.Optional;

public class AvroSchema {

	private String schemaHash;
	private Schema schema;
	private Optional<String> schemaVersion; // read from schema, can be null
	private EventType eventType;
	private EventStructure eventStructure;

	public AvroSchema(String schemaHash, Schema schema, Optional<String> schemaVersion, EventType eventType) {
		this.schemaHash = schemaHash;
		this.eventType = eventType;
		this.schema = schema;
		this.schemaVersion = schemaVersion;
		this.eventStructure = new EventStructure(this); 
	}

	public String getName() {
		return schemaHash;
	}
	
	public String getFullSchemaName() {
		return schema.getFullName();
	}
	
	
	public Optional<String> getSchemaVersion() {
		return schemaVersion;
	}

	public String getSchemaHash() {
		return schemaHash;
	}

	public Schema getSchema() {
		return schema;
	}

	public EventType getEventType() {
		return eventType;
	}

	public EventStructure getEventStructure() {
		return eventStructure;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((schemaHash == null) ? 0 : schemaHash.hashCode());
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
		AvroSchema other = (AvroSchema) obj;
		if (schemaHash == null) {
			if (other.schemaHash != null)
				return false;
		} else if (!schemaHash.equals(other.schemaHash))
			return false;
		return true;
	}
	
}
