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

public class DwhTableGenerator {

	private static final Logger log = LogManager.getLogger(DwhTableGenerator.class.getName());
	
	private static final String COLUMNS_PLACEHOLDER = "__COLUMNS_PLACEHOLDER__";
	private static final String TABLENAME_PLACEHOLDER = "__TABLENAME_PLACEHOLDER__";
	private static final String SCHEMA_PLACEHOLDER = "__SCHEMA_PLACEHOLDER__";
	
	private static final String TABLE_SCHEMA = "json";
	private static final String FALLBACK_COLUMN_TYPE = "NVARCHAR(255)";
	
	
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
			String columnName = leafNode.getId().replace(".", "_");
			Optional<String> columnTypeOption = dwhTableTypeForNode(leafNode);
			String columnType = columnTypeOption.isPresent() ? columnTypeOption.get() : FALLBACK_COLUMN_TYPE;
			columns.put(columnName, columnType);
		}
		String columnList = "";
		for (String columnName : CollectionUtil.toSortedList(columns.keySet())) {
			columnList += "  , " + columnName + " " + columns.get(columnName) + " NULL\n";
		}

		// TODO STOPPED HERE

		String ddl = FileUtil.loadFromResources(Config.getInstance().DWH_TABLE_TEMPLATE_FILE);
		ddl = ddl.replace(SCHEMA_PLACEHOLDER, TABLE_SCHEMA);
		ddl = ddl.replace(TABLENAME_PLACEHOLDER, eventType.getName());
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

	public static Optional<String> jsonTypeForNode(Node node) {
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
}
