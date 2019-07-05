package de.esailors.dataheart.drillviews.processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.EventStructure;
import de.esailors.dataheart.drillviews.data.EventType;
import de.esailors.dataheart.drillviews.data.Node;
import de.esailors.dataheart.drillviews.data.NodePropertyType;
import de.esailors.dataheart.drillviews.util.CollectionUtil;
import de.esailors.dataheart.drillviews.util.JsonUtil.JsonType;

public class DwhGenerator {

	private static final Logger log = LogManager.getLogger(DwhGenerator.class.getName());

	private static final String COLUMNS_PLACEHOLDER = "__COLUMNS_PLACEHOLDER__";
	private static final String TABLENAME_PLACEHOLDER = "__TABLENAME_PLACEHOLDER__";
	private static final String SCHEMA_PLACEHOLDER = "__SCHEMA_PLACEHOLDER__";

	private static final String JOB_NAME_PLACEHOLDER = "__JOB_NAME_PLACEHOLDER__";
	private static final String EVENT_TYPE_PLACEHOLDER = "__EVENT_TYPE_PLACEHOLDER__";
	private static final String TABLE_NAME_PLACEHOLDER = "__TABLE_NAME_PLACEHOLDER__";

//	private static final String DWH_TABLE_NAME_PREFIX = "andre_";
	private static final String DWH_TABLE_NAME_PREFIX = "";
	private static final String DWH_TABLE_FALLBACK_COLUMN_TYPE = "NVARCHAR(255)";

	private static final String JOB_NAME_PREFIX = "DWHSRC_HBASE_LOADJSON_";
	private static final String JOB_FALLBACK_DATA_TYPE = "String";

	public enum PrimitiveType {
		// TODO doesn't belong here
		// TODO for avro we can distinguish between int and bigint
		INT, FLOAT, TEXT, BOOLEAN// ?, ARRAY
	}

	public String createDwhJob(EventType eventType) {

		Optional<EventStructure> mergedEventStructuredOption = eventType.getMergedEventStructured();
		if (!mergedEventStructuredOption.isPresent()) {
			throw new IllegalStateException(
					"Expect mergedEventStructure to exist to create a job out of for: " + eventType.toString());
		}
		EventStructure eventStructure = mergedEventStructuredOption.get();
		Set<Node> leafNodes = eventStructure.getEventStructureTree().getLeafNodes();

		String jobName = JOB_NAME_PREFIX + eventType.getName().toUpperCase();
		String columns = "";

		Map<String, Node> columnContent = new HashMap<>();
		for (Node node : leafNodes) {
			String columnName = columnNameForNode(node);
			columnContent.put(columnName, node);
		}

		int index = 1; // 0 taken for _RUN_ID_ in template
		for (String columnName : CollectionUtil.toSortedList(columnContent.keySet())) {

			Node node = columnContent.get(columnName);

			Optional<String> jsonTableTypeForNode = dwhJobDataTypeForNode(node);
			String dataType = jsonTableTypeForNode.isPresent() ? jsonTableTypeForNode.get() : JOB_FALLBACK_DATA_TYPE;

			columns += ", ('" + jobName + "', 'JSONPARSER', " + index + ", 'Target Column', '" + columnName + "')\n";
			columns += ", ('" + jobName + "', 'JSONPARSER', " + index + ", 'JsonPath', '$." + node.getId() + "')\n";
			columns += ", ('" + jobName + "', 'JSONPARSER', " + index + ", 'Type', '" + dataType + "')\n\n";

			index++;
		}

		String job = FileUtil.loadFromResources(Config.getInstance().DWH_JOB_TEMPLATE_FILE);

		job = job.replace(JOB_NAME_PLACEHOLDER, jobName);
		job = job.replace(EVENT_TYPE_PLACEHOLDER, eventType.getName().toUpperCase());
		job = job.replace(TABLE_NAME_PLACEHOLDER, dwhTableNameFor(eventType));
		job = job.replace(COLUMNS_PLACEHOLDER, columns);

		return job;
	}

	private Optional<String> dwhJobDataTypeForNode(Node node) {
		Optional<PrimitiveType> primitiveTypeOption = primitiveTypeForNode(node);

		if (!primitiveTypeOption.isPresent()) {
			return Optional.absent();
		}
		PrimitiveType primitiveType = primitiveTypeOption.get();
		switch (primitiveType) {
		case TEXT:
			return Optional.of("String");
		case INT:
			return Optional.of("Integer");
		case FLOAT:
			return Optional.of("Number");
		case BOOLEAN:
			return Optional.of("Boolean");
		default: {
			log.warn("Unsupported primitiveType " + primitiveType + " for node: " + node.getId());
			return Optional.absent();
		}
		}
	}

	public String createDwhTable(EventType eventType) {

		Optional<EventStructure> mergedEventStructuredOption = eventType.getMergedEventStructured();
		if (!mergedEventStructuredOption.isPresent()) {
			throw new IllegalStateException(
					"Expect mergedEventStructure to exist to create a ddl out of for: " + eventType.toString());
		}
		EventStructure eventStructure = mergedEventStructuredOption.get();
		Set<Node> leafNodes = eventStructure.getEventStructureTree().getLeafNodes();
		Map<String, String> columns = new HashMap<>();
		for (Node leafNode : leafNodes) {
			String columnName = columnNameForNode(leafNode);
			Optional<String> columnTypeOption = dwhTableDataTypeForNode(leafNode);
			String columnType = columnTypeOption.isPresent() ? columnTypeOption.get() : DWH_TABLE_FALLBACK_COLUMN_TYPE;
			columns.put(columnName, columnType);
		}
		String columnList = "";
		for (String columnName : CollectionUtil.toSortedList(columns.keySet())) {
			columnList += ", [" + columnName + "] " + columns.get(columnName) + " NULL\n";
		}

		String ddl = FileUtil.loadFromResources(Config.getInstance().DWH_TABLE_TEMPLATE_FILE);
		ddl = ddl.replace(SCHEMA_PLACEHOLDER, Config.getInstance().DWH_TABLE_SCHEMA);
		ddl = ddl.replace(TABLENAME_PLACEHOLDER, dwhTableNameFor(eventType));
		ddl = ddl.replace(COLUMNS_PLACEHOLDER, columnList);

		return ddl;
	}

	private Optional<String> dwhTableDataTypeForNode(Node node) {
		Optional<PrimitiveType> primitiveTypeOption = primitiveTypeForNode(node);

		if (!primitiveTypeOption.isPresent()) {
			return Optional.absent();
		}
		PrimitiveType primitiveType = primitiveTypeOption.get();
		switch (primitiveType) {
		case TEXT:
			return Optional.of("NVARCHAR(255)");
		case INT:
			return Optional.of("BIGINT");
		case FLOAT:
			return Optional.of("NUMERIC(18, 5)");
		case BOOLEAN:
			return Optional.of("BIT");
		default: {
			log.warn("Unsupported primitiveType " + primitiveType + " for node: " + node.getId());
			return Optional.absent();
		}
		}
	}

	private Optional<PrimitiveType> primitiveTypeForNode(Node node) {
		// TODO MOVE THIS TO NODE OR SOMETHING!
		// TODO multiple types, no type, avro_type, union_types etc

		// try avro type first
		Optional<Type> avroTypeOption = primitiveAvroTypeForNode(node);
		if (!avroTypeOption.isPresent()) {
			log.debug("Unable to determined primitivte avroType for " + node.getId());
		} else {
			Type avroType = avroTypeOption.get();
			log.debug("Determined primitive avroType " + avroType + " for " + node.getId());
			switch (avroType) {
			case STRING:
				return Optional.of(PrimitiveType.TEXT);
			case ENUM:
				// TODO special handling for ENUMs?
				return Optional.of(PrimitiveType.TEXT);
			case BOOLEAN:
				return Optional.of(PrimitiveType.BOOLEAN);
			case INT:
				return Optional.of(PrimitiveType.INT);
			case LONG:
				// TODO maybe it makes sense to differentiate between int and long here
				return Optional.of(PrimitiveType.INT);
			case FLOAT:
				return Optional.of(PrimitiveType.FLOAT);
			case DOUBLE:
				// TODO maybe it makes sense to differentiate between float and double here
				return Optional.of(PrimitiveType.FLOAT);
				
			case MAP:
			case RECORD:
			case UNION: {
				throw new IllegalStateException("Unexpected nested avro type received as primitive " + avroType +" for node " + node.getId());
			}
				
			case BYTES:
			case FIXED:
			case ARRAY:
			case NULL:
			default: {
				log.warn("Unsupported avro type detected: " + avroType + " for " + node.getId());
			}
			}
		}

		Optional<JsonType> jsonTypeOption = primitiveJsonTypeForNode(node);
		if (!jsonTypeOption.isPresent()) {
			return Optional.absent();
		}
		JsonType jsonType = jsonTypeOption.get();
		switch (jsonType) {
		case TEXTUAL:
			return Optional.of(PrimitiveType.TEXT);
		case INTEGER:
			return Optional.of(PrimitiveType.INT);
		case FLOAT:
			return Optional.of(PrimitiveType.FLOAT);
		case BOOLEAN:
			return Optional.of(PrimitiveType.BOOLEAN);
		default: {
			log.warn("Unsupported jsonType " + jsonType + " for node: " + node.getId());
			return Optional.absent();
		}
		}
	}

	private Optional<Type> primitiveAvroTypeForNode(Node node) {
		Set<String> avroTypes = node.getProperty(NodePropertyType.AVRO_TYPE);
		if (avroTypes == null || avroTypes.size() == 0) {
			log.debug("Did not find any avro Type property for " + node.getId());
			return Optional.absent();
		}

		String primitiveAvroType = null;
		for (String avroType : avroTypes) {
			if (Type.NULL.toString().equals(avroType)) {
				continue;
			}
			if (primitiveAvroType == null) {
				primitiveAvroType = avroType;
			} else {
				// TODO bad heuristic, if we get conflicting types we should get up to String as
				// well
				if (Type.STRING.toString().equals(avroType)) {
					primitiveAvroType = avroType;
				}
			}
		}
		if (primitiveAvroType == null) {
			log.debug("Unable to determine primitive avro type for node: " + node.getId());
			return Optional.absent();
		}
		if (Type.UNION.toString().equals(primitiveAvroType)) {
			// TODO look at union types
			Set<String> unionTypes = node.getProperty(NodePropertyType.AVRO_UNION_TYPE);
			if (unionTypes == null || unionTypes.size() == 0) {
				throw new IllegalStateException(
						"Expect nodes with avro type UNION to have set union types property " + node.getId());
			}
			for (String unionType : unionTypes) {
				if (Type.NULL.toString().equals(unionType)) {
					continue;
				}
				// TODO bad heuristic, same as above
				if (Type.UNION.toString().equals(primitiveAvroType)
						|| Type.STRING.toString().equals(primitiveAvroType)) {
					primitiveAvroType = unionType;
				}
			}
		}
		return Optional.of(Type.valueOf(primitiveAvroType));
	}

	public Optional<JsonType> primitiveJsonTypeForNode(Node node) {
		// TODO MOVE THIS TO NODE OR SOMETHING!
		// TODO multiple types, no type, avro_type, union_types etc
		if (node.hasChildren()) {
			throw new IllegalArgumentException("Got asked for a primitive type of a nested node: " + node.getId());
		}
		Set<String> jsonTypes = node.getProperty(NodePropertyType.JSON_TYPE);
		if (jsonTypes == null || jsonTypes.size() == 0) {
			log.warn("Did not find any JsonTypes property for " + node.getId());
			return Optional.absent();
		}
		String primitiveJsonType = null;
		for (String jsonType : jsonTypes) {
			if (JsonType.NULL.toString().equals(jsonType)) {
				continue;
			}
			if (primitiveJsonType == null) {
				primitiveJsonType = jsonType;
			} else {
				// textual always wins
				// TODO if we get any two conflicting types we should return textual as well
				if (JsonType.TEXTUAL.toString().equals(jsonType)) {
					primitiveJsonType = jsonType;
				}
			}
		}
		if (primitiveJsonType == null) {
			log.debug("Unable to determine primitive json type for node: " + node.getId());
			return Optional.absent();
		}
		return Optional.of(JsonType.valueOf(primitiveJsonType));
	}

	private String columnNameForNode(Node node) {
		return node.getId().replaceAll("\\.", "_");
	}

	private String dwhTableNameFor(EventType eventType) {
		return DWH_TABLE_NAME_PREFIX + eventType.getName();
	}
}
