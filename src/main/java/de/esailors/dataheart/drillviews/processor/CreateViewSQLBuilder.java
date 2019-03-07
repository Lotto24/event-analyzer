package de.esailors.dataheart.drillviews.processor;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonNode;

import de.esailors.dataheart.drillviews.data.Event;

public class CreateViewSQLBuilder {

	private static final Logger log = LogManager.getLogger(CreateViewSQLBuilder.class.getName());

	// TODO move these to Config

	private static final String HBASE_TABLE = "kafka_events";
	private static final String HBASE_COLUMN_FAMILY = "d";
	private static final String HBASE_JSON_FIELD = "json";

	private static final String DRILL_HBASE_STORAGE_PLUGIN_NAME = "hbase";

	private static final String DRILL_VIEW_WORKSPACE = "drill.views";
	private static final String DRILL_VIEW_SUBFOLDER = "json_events";

	private static final String ROW_TIMESTAMP_ALIAS = "row_timestamp";

	private static final String SUBSELECT_ALIAS = "e";
	private static final String JSON_FIELD_ALIAS = "json";

	private static final String EVENT_TYPE_FIELD = "eventType";

	private static final int IDENTATION = 4;

	public static String generateDrillViewsFor(Event event) {
		if (event == null) {
			throw new IllegalArgumentException("null given");
		}

		log.info("Generating create view statement for Event from " + event.getTopic().getTopicName());
		// TODO generate drill views from avro schema if possible

		JsonNode json = event.getEventJson();
		String viewName = event.getTopic().getTopicName();

		StringBuilder viewBuilder = new StringBuilder();

		generateCommentBlock(viewBuilder, viewName, json);

		generateView(json, viewName, viewBuilder, null, null);
		generateView(json, viewName, viewBuilder, "'-1' day", "last_day");
		generateView(json, viewName, viewBuilder, "'-7' day", "last_week");

		return viewBuilder.toString();

	}

	private static void generateView(JsonNode json, String viewName, StringBuilder viewBuilder, String timeLimit,
			String subfolder) {
		generateViewStart(viewBuilder, viewName, subfolder);

		String fieldPrefix = SUBSELECT_ALIAS + "." + JSON_FIELD_ALIAS + ".";

		generateSelectColumns(json, viewBuilder, fieldPrefix, "");

		String eventType = json.get(EVENT_TYPE_FIELD).asText().toUpperCase();

		generateViewEnd(viewBuilder, eventType, timeLimit);
	}

	private static void generateCommentBlock(StringBuilder viewBuilder, String viewName, JsonNode json) {
		viewBuilder.append("/*\n");
		viewBuilder.append("Auto-generated view for ");
		viewBuilder.append(viewName);
//		viewBuilder.append("\nSample event used for view generation:\n");
//		viewBuilder.append(JsonPrettyPrinter.prettyPrintJsonString(json));
		viewBuilder.append("\n*/\n\n");
	}

	private static void generateSelectColumns(JsonNode json, StringBuilder viewBuilder, String fieldPrefix,
			String keyPrefix) {

		Iterator<Entry<String, JsonNode>> fields = json.getFields();
		while (fields.hasNext()) {
			Entry<String, JsonNode> entry = fields.next();

			if (entry.getValue().isObject()) {
				// recursive call
				String newKeyPrefix = keyPrefix + entry.getKey() + "_";
				String newFieldPrefix = fieldPrefix + "`" + entry.getKey() + "`.";
				generateSelectColumns(entry.getValue(), viewBuilder, newFieldPrefix, newKeyPrefix);
			} else {
				viewBuilder.append(",\n");
				viewBuilder.append(ident());
				viewBuilder.append(fieldPrefix);
				viewBuilder.append("`");
				viewBuilder.append(entry.getKey());
				viewBuilder.append("` as `");
				viewBuilder.append(keyPrefix);
				viewBuilder.append(entry.getKey());
				viewBuilder.append("`");
			}
		}
	}

	private static void generateViewStart(StringBuilder viewBuilder, String viewName, String subFolder) {
		viewBuilder.append("CREATE OR REPLACE VIEW\n");
		viewBuilder.append(ident());
		viewBuilder.append(DRILL_VIEW_WORKSPACE);
		viewBuilder.append(".`");
		viewBuilder.append(DRILL_VIEW_SUBFOLDER);
		viewBuilder.append("/");
		if (subFolder != null) {
			viewBuilder.append(subFolder);
			viewBuilder.append("/");
		}
		viewBuilder.append(viewName);
		viewBuilder.append("` AS\n");
		viewBuilder.append("SELECT\n");
		viewBuilder.append(ident());
		viewBuilder.append("CONVERT_FROM(");
		viewBuilder.append(SUBSELECT_ALIAS);
		viewBuilder.append(".row_key, 'UTF8') as row_key,\n");
		viewBuilder.append(ident());
		viewBuilder.append("TO_TIMESTAMP(CAST(SUBSTR(CONVERT_FROM(");
		viewBuilder.append(SUBSELECT_ALIAS);
		viewBuilder.append(".row_key, 'UTF8'), STRPOS(CONVERT_FROM(");
		viewBuilder.append(SUBSELECT_ALIAS);
		viewBuilder.append(".row_key, 'UTF8'), '-') + 1, 10) AS BIGINT)) as ");
		viewBuilder.append(ROW_TIMESTAMP_ALIAS);
	}

	private static void generateViewEnd(StringBuilder viewBuilder, String eventType, String timeLimit) {
		viewBuilder.append("\nFROM (\n");
		viewBuilder.append(ident());
		viewBuilder.append("SELECT\n");
		viewBuilder.append(ident());
		viewBuilder.append(ident());
		viewBuilder.append("row_key,\n");
		viewBuilder.append(ident());
		viewBuilder.append(ident());
		viewBuilder.append("CONVERT_FROM(");
		viewBuilder.append(HBASE_TABLE);
		viewBuilder.append(".");
		viewBuilder.append(HBASE_COLUMN_FAMILY);
		viewBuilder.append(".");
		viewBuilder.append(HBASE_JSON_FIELD);
		viewBuilder.append(", 'JSON') AS ");
		viewBuilder.append(JSON_FIELD_ALIAS);
		viewBuilder.append("\n");
		viewBuilder.append(ident());
		viewBuilder.append("FROM\n");
		viewBuilder.append(ident());
		viewBuilder.append(ident());
		viewBuilder.append(DRILL_HBASE_STORAGE_PLUGIN_NAME);
		viewBuilder.append(".");
		viewBuilder.append(HBASE_TABLE);
		viewBuilder.append("\n");
		viewBuilder.append(ident());
		viewBuilder.append("WHERE\n");
		viewBuilder.append(ident());
		viewBuilder.append(ident());
		viewBuilder.append("CONVERT_FROM(row_key, 'UTF8') BETWEEN '");
		viewBuilder.append(eventType);
		viewBuilder.append("-");
		generateRowKeyStart(viewBuilder, timeLimit);
		viewBuilder.append(" AND '");
		viewBuilder.append(eventType);
		viewBuilder.append("-9'\n");
		viewBuilder.append(") ");
		viewBuilder.append(SUBSELECT_ALIAS);
		viewBuilder.append(";\n");
	}

	private static void generateRowKeyStart(StringBuilder viewBuilder, String timeLimit) {
		String rowKeyStart;
		if (timeLimit == null) {
			rowKeyStart = "0'";
		} else {
			rowKeyStart = "' || UNIX_TIMESTAMP(TO_CHAR(DATE_ADD(now(), interval " + timeLimit
					+ "),'yyyy-MM-dd HH:mm:ss'))";
		}

		viewBuilder.append(rowKeyStart);
	}

	private static Object ident() {
		StringBuilder r = new StringBuilder();
		for (int i = 0; i < IDENTATION; i++) {
			r.append(" ");
		}
		return r.toString();
	}

}
