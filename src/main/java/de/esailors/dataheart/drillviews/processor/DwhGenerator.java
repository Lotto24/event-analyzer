package de.esailors.dataheart.drillviews.processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
			
			Optional<String> jsonTableTypeForNode = dataTypeForNode(node);
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

	private Optional<String> dataTypeForNode(Node node) {
		Optional<String> jsonTypeForNodeOption = jsonTypeForNode(node);
		if (!jsonTypeForNodeOption.isPresent()) {
			log.warn("Unable to detect json type for: " + node.getId());
			return Optional.absent();
		}
		String jsonType = jsonTypeForNodeOption.get();
		switch (JsonType.valueOf(jsonType)) {
		case TEXTUAL: return Optional.of("String");
		case INTEGER: return Optional.of("Integer");
		case FLOAT: return Optional.of("Number");
		case BOOLEAN: return Optional.of("Boolean");
		default: {
			log.warn("Unsupported jsonType " + jsonType + " for node: " + node.getId());
			return Optional.absent();
		}
	}
	}
	
	public String createDwhTable(EventType eventType) {
		// TODO quick n dirty for now

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
			Optional<String> columnTypeOption = dwhTableTypeForNode(leafNode);
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

	private Optional<String> dwhTableTypeForNode(Node node) {
		Optional<String> jsonTypeOption = jsonTypeForNode(node);
		if(!jsonTypeOption.isPresent()) {
			return Optional.absent();
		}
		String jsonType = jsonTypeOption.get();
		switch (JsonType.valueOf(jsonType)) {
			case TEXTUAL: return Optional.of("NVARCHAR(255)");
			case INTEGER: return Optional.of("BIGINT");
			case FLOAT: return Optional.of("NUMERIC(18, 5)");
			case BOOLEAN: return Optional.of("BIT");
			default: {
				log.warn("Unsupported jsonType " + jsonType + " for node: " + node.getId());
				return Optional.absent();
			}
		}
	}

	public Optional<String> jsonTypeForNode(Node node) {
		// TODO MOVE THIS TO NODE OR SOMETHING!
		// TODO multiple types, no type, avro_type, union_types etc
		// TODO could not be more hacky :D
		Set<String> jsonTypes = node.getProperty(NodePropertyType.JSON_TYPE);
		if (jsonTypes == null || jsonTypes.size() == 0) {
			log.warn("Did not find any JsonTypes property for " + node.getId());
			return Optional.absent();
		}
		String columnType = null;
		for (String jsonType : jsonTypes) {
			if (JsonType.NULL.toString().equals(jsonType)) {
				continue;
			}
			if (columnType == null) {
				columnType = jsonType;
			} else {
				// textual always wins
				if (JsonType.TEXTUAL.toString().equals(jsonType.toString())) {
					columnType = jsonType;
				}
			}
		}
		if (columnType == null) {
			log.warn("Unable to determine dwh table type for column: " + node.getId());
			return Optional.absent();
		}
		return Optional.of(columnType);
	}
	
	private String columnNameForNode(Node node) {
		return node.getId().replaceAll("\\.", "_");
	}
	
	private String dwhTableNameFor(EventType eventType) {
		return DWH_TABLE_NAME_PREFIX + eventType.getName();
	}
}
